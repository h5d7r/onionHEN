/* Copyright (C) 2025 OnionHEN / LightningMods
 *
 * Built-in anonymous FTP server on port 2121.
 * Runs inside the util daemon. Supports PASV transfers and minimal commands.
 */

#include "ftp_server.hpp"
#include "common_utils.h"
#include "onion_cjson.hpp"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include "cJSON.hpp"
}

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0x20000
#endif

namespace {

constexpr int kFtpPort = 2121;
constexpr int kFtpBacklog = 8;
constexpr int kTransferBufferSize = 8192;

std::atomic<bool> g_running{false};
std::atomic<bool> g_should_stop{false};
int g_listen_fd = -1;
std::thread g_accept_thread;
pthread_mutex_t g_state_mutex = PTHREAD_MUTEX_INITIALIZER;

void ftp_send(int fd, int code, const char *text) {
  char line[512];
  std::snprintf(line, sizeof(line), "%d %s\r\n", code, text);
  (void)send(fd, line, std::strlen(line), MSG_NOSIGNAL);
}

void ftp_send_multiline(int fd, int code, const char *text) {
  char line[512];
  std::snprintf(line, sizeof(line), "%d-%s\r\n", code, text);
  (void)send(fd, line, std::strlen(line), MSG_NOSIGNAL);
}

void ftp_send_end(int fd, int code, const char *text) {
  char line[512];
  std::snprintf(line, sizeof(line), "%d %s\r\n", code, text);
  (void)send(fd, line, std::strlen(line), MSG_NOSIGNAL);
}

// Read one CRLF-terminated line. Returns true if a complete line was read.
bool read_line(int fd, std::string &out) {
  char ch;
  out.clear();
  while (true) {
    ssize_t n = recv(fd, &ch, 1, 0);
    if (n <= 0)
      return false;
    if (ch == '\r') {
      // Look ahead for \n.
      n = recv(fd, &ch, 1, 0);
      if (n <= 0)
        return false;
      if (ch == '\n')
        return true;
      out.push_back('\r');
      out.push_back(ch);
    } else if (ch == '\n') {
      return true;
    } else {
      out.push_back(ch);
    }
  }
}

// Trim trailing spaces.
std::string trim_right(std::string s) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
    s.pop_back();
  return s;
}

std::string normalize_path(const std::string &cwd, const std::string &arg) {
  std::string base = (arg.empty() || arg[0] != '/') ? cwd : "";
  if (!base.empty() && base.back() != '/')
    base += '/';
  std::string combined = base + arg;

  std::vector<std::string> parts;
  size_t start = 0;
  while (start < combined.size()) {
    size_t end = combined.find('/', start);
    if (end == std::string::npos)
      end = combined.size();
    std::string part = combined.substr(start, end - start);
    if (part == "..") {
      if (!parts.empty())
        parts.pop_back();
    } else if (!part.empty() && part != ".") {
      parts.push_back(part);
    }
    start = end + 1;
  }

  std::string result = "/";
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i != 0)
      result += '/';
    result += parts[i];
  }
  return result;
}

// Open a PASV data socket, bind to an ephemeral port, and listen.
// Returns the data fd and fills host_ip/port. On failure returns -1.
int open_pasv_socket(std::string &host_ip, int &port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  int opt = 1;
  (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = 0;

  if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  socklen_t len = sizeof(addr);
  if (getsockname(fd, reinterpret_cast<struct sockaddr *>(&addr), &len) < 0) {
    close(fd);
    return -1;
  }

  if (listen(fd, 1) < 0) {
    close(fd);
    return -1;
  }

  port = ntohs(addr.sin_port);

  char ip_buf[40] = {};
  if (get_ip_address(ip_buf, sizeof(ip_buf)) < 0)
    std::strncpy(ip_buf, "127.0.0.1", sizeof(ip_buf) - 1);
  host_ip = ip_buf;

  return fd;
}

// Accept one data connection with a timeout.
int accept_pasv_data(int pasv_fd, int timeout_ms) {
  fd_set fds;
  FD_ZERO(&fds);
  FD_SET(pasv_fd, &fds);

  struct timeval tv;
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;

  int ret = select(pasv_fd + 1, &fds, nullptr, nullptr, &tv);
  if (ret <= 0)
    return -1;

  struct sockaddr_in client_addr {};
  socklen_t len = sizeof(client_addr);
  return accept(pasv_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
                &len);
}

void send_file(int data_fd, const std::string &path) {
  int file_fd = open(path.c_str(), O_RDONLY);
  if (file_fd < 0)
    return;

  char buf[kTransferBufferSize];
  ssize_t n;
  while ((n = read(file_fd, buf, sizeof(buf))) > 0) {
    ssize_t sent = 0;
    while (sent < n) {
      ssize_t r = send(data_fd, buf + sent, static_cast<size_t>(n - sent),
                       MSG_NOSIGNAL);
      if (r <= 0) {
        close(file_fd);
        return;
      }
      sent += r;
    }
  }
  close(file_fd);
}

void recv_file(int data_fd, const std::string &path) {
  int file_fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (file_fd < 0)
    return;

  char buf[kTransferBufferSize];
  ssize_t n;
  while ((n = recv(data_fd, buf, sizeof(buf), 0)) > 0) {
    ssize_t written = 0;
    while (written < n) {
      ssize_t r = write(file_fd, buf + written, static_cast<size_t>(n - written));
      if (r <= 0) {
        close(file_fd);
        return;
      }
      written += r;
    }
  }
  close(file_fd);
}

std::string format_time(time_t t) {
  struct tm tm_info {};
  localtime_r(&t, &tm_info);
  char buf[64];
  std::strftime(buf, sizeof(buf), "%b %d %H:%M", &tm_info);
  return buf;
}

void send_listing(int data_fd, const std::string &path) {
  DIR *dir = opendir(path.c_str());
  if (!dir)
    return;

  char line[1024];
  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (std::strcmp(entry->d_name, ".") == 0 ||
        std::strcmp(entry->d_name, "..") == 0)
      continue;

    std::string full = path;
    if (full.back() != '/')
      full += '/';
    full += entry->d_name;

    struct stat st {};
    if (stat(full.c_str(), &st) < 0)
      continue;

    const char *type = S_ISDIR(st.st_mode) ? "d" : "-";
    const char *perms = S_ISDIR(st.st_mode) ? "rwxr-xr-x" : "rw-r--r--";
    std::string mtime = format_time(st.st_mtime);

    std::snprintf(line, sizeof(line), "%s%s 1 ftp ftp %lld %s %s\r\n", type,
                  perms, static_cast<long long>(st.st_size), mtime.c_str(),
                  entry->d_name);

    size_t len = std::strlen(line);
    size_t sent = 0;
    while (sent < len) {
      ssize_t r = send(data_fd, line + sent, len - sent, MSG_NOSIGNAL);
      if (r <= 0) {
        closedir(dir);
        return;
      }
      sent += static_cast<size_t>(r);
    }
  }
  closedir(dir);
}

struct ClientSession {
  int control_fd = -1;
  std::string cwd = "/";
  std::string rename_from;
  int pasv_fd = -1;
  int pasv_port = 0;
  std::string pasv_ip;
};

void handle_client(int control_fd) {
  ClientSession session;
  session.control_fd = control_fd;

  ftp_send(control_fd, 220, "OnionHEN FTP server ready");

  std::string line;
  while (read_line(control_fd, line)) {
    // Split command and argument.
    std::string cmd;
    std::string arg;
    size_t space = line.find(' ');
    if (space == std::string::npos) {
      cmd = line;
    } else {
      cmd = line.substr(0, space);
      arg = trim_right(line.substr(space + 1));
    }

    // Uppercase command.
    for (auto &c : cmd)
      c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

    if (cmd == "USER") {
      ftp_send(control_fd, 331, "Please specify the password");
    } else if (cmd == "PASS") {
      ftp_send(control_fd, 230, "Login successful");
    } else if (cmd == "SYST") {
      ftp_send(control_fd, 215, "UNIX Type: L8");
    } else if (cmd == "FEAT") {
      ftp_send_multiline(control_fd, 211, "Features:");
      ftp_send_multiline(control_fd, 211, " PASV");
      ftp_send_end(control_fd, 211, "End");
    } else if (cmd == "PWD") {
      char reply[512];
      std::snprintf(reply, sizeof(reply), "\"%s\" is the current directory",
                    session.cwd.c_str());
      ftp_send(control_fd, 257, reply);
    } else if (cmd == "CWD") {
      std::string target = normalize_path(session.cwd, arg);
      struct stat st {};
      if (stat(target.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        session.cwd = target;
        ftp_send(control_fd, 250, "Directory successfully changed");
      } else {
        ftp_send(control_fd, 550, "Failed to change directory");
      }
    } else if (cmd == "TYPE") {
      if (arg == "I" || arg == "A") {
        ftp_send(control_fd, 200, "Switching to Type I");
      } else {
        ftp_send(control_fd, 504, "Command not implemented for that parameter");
      }
    } else if (cmd == "PASV") {
      if (session.pasv_fd >= 0) {
        close(session.pasv_fd);
        session.pasv_fd = -1;
      }
      session.pasv_fd = open_pasv_socket(session.pasv_ip, session.pasv_port);
      if (session.pasv_fd < 0) {
        ftp_send(control_fd, 425, "Can't open data connection");
      } else {
        char reply[256];
        unsigned int h1, h2, h3, h4, p1, p2;
        ::sscanf(session.pasv_ip.c_str(), "%u.%u.%u.%u", &h1, &h2, &h3, &h4);
        p1 = session.pasv_port / 256;
        p2 = session.pasv_port % 256;
        std::snprintf(reply, sizeof(reply),
                      "Entering Passive Mode (%u,%u,%u,%u,%u,%u)", h1, h2, h3,
                      h4, p1, p2);
        ftp_send(control_fd, 227, reply);
      }
    } else if (cmd == "LIST" || cmd == "NLST") {
      std::string target = arg.empty() ? session.cwd
                                       : normalize_path(session.cwd, arg);
      int data_fd = accept_pasv_data(session.pasv_fd, 20000);
      if (data_fd < 0) {
        ftp_send(control_fd, 425, "Can't open data connection");
      } else {
        ftp_send(control_fd, 150, "Here comes the directory listing");
        send_listing(data_fd, target);
        close(data_fd);
        close(session.pasv_fd);
        session.pasv_fd = -1;
        ftp_send(control_fd, 226, "Directory send OK");
      }
    } else if (cmd == "RETR") {
      std::string target = normalize_path(session.cwd, arg);
      struct stat st {};
      if (stat(target.c_str(), &st) < 0 || S_ISDIR(st.st_mode)) {
        ftp_send(control_fd, 550, "Failed to open file");
      } else {
        int data_fd = accept_pasv_data(session.pasv_fd, 20000);
        if (data_fd < 0) {
          ftp_send(control_fd, 425, "Can't open data connection");
        } else {
          ftp_send(control_fd, 150, "Opening data connection");
          send_file(data_fd, target);
          close(data_fd);
          close(session.pasv_fd);
          session.pasv_fd = -1;
          ftp_send(control_fd, 226, "Transfer complete");
        }
      }
    } else if (cmd == "STOR") {
      std::string target = normalize_path(session.cwd, arg);
      int data_fd = accept_pasv_data(session.pasv_fd, 20000);
      if (data_fd < 0) {
        ftp_send(control_fd, 425, "Can't open data connection");
      } else {
        ftp_send(control_fd, 150, "Opening data connection");
        recv_file(data_fd, target);
        close(data_fd);
        close(session.pasv_fd);
        session.pasv_fd = -1;
        ftp_send(control_fd, 226, "Transfer complete");
      }
    } else if (cmd == "DELE") {
      std::string target = normalize_path(session.cwd, arg);
      if (unlink(target.c_str()) == 0) {
        ftp_send(control_fd, 250, "Delete operation successful");
      } else {
        ftp_send(control_fd, 550, "Delete operation failed");
      }
    } else if (cmd == "MKD") {
      std::string target = normalize_path(session.cwd, arg);
      if (mkdir(target.c_str(), 0755) == 0) {
        char reply[512];
        std::snprintf(reply, sizeof(reply), "\"%s\" directory created",
                      target.c_str());
        ftp_send(control_fd, 257, reply);
      } else {
        ftp_send(control_fd, 550, "Create directory operation failed");
      }
    } else if (cmd == "RMD") {
      std::string target = normalize_path(session.cwd, arg);
      if (rmdir(target.c_str()) == 0) {
        ftp_send(control_fd, 250, "Remove directory operation successful");
      } else {
        ftp_send(control_fd, 550, "Remove directory operation failed");
      }
    } else if (cmd == "SIZE") {
      std::string target = normalize_path(session.cwd, arg);
      struct stat st {};
      if (stat(target.c_str(), &st) == 0 && !S_ISDIR(st.st_mode)) {
        char reply[64];
        std::snprintf(reply, sizeof(reply), "%lld",
                      static_cast<long long>(st.st_size));
        ftp_send(control_fd, 213, reply);
      } else {
        ftp_send(control_fd, 550, "Could not get file size");
      }
    } else if (cmd == "RNFR") {
      session.rename_from = normalize_path(session.cwd, arg);
      ftp_send(control_fd, 350, "Ready for RNTO");
    } else if (cmd == "RNTO") {
      std::string target = normalize_path(session.cwd, arg);
      if (!session.rename_from.empty() &&
          rename(session.rename_from.c_str(), target.c_str()) == 0) {
        ftp_send(control_fd, 250, "Rename successful");
      } else {
        ftp_send(control_fd, 550, "Rename failed");
      }
      session.rename_from.clear();
    } else if (cmd == "CDUP") {
      session.cwd = normalize_path(session.cwd, "..");
      ftp_send(control_fd, 250, "Directory successfully changed");
    } else if (cmd == "NOOP") {
      ftp_send(control_fd, 200, "OK");
    } else if (cmd == "QUIT") {
      ftp_send(control_fd, 221, "Goodbye");
      break;
    } else {
      ftp_send(control_fd, 502, "Command not implemented");
    }
  }

  if (session.pasv_fd >= 0)
    close(session.pasv_fd);
  close(control_fd);
}

void accept_loop() {
  while (!g_should_stop.load()) {
    struct sockaddr_in client_addr {};
    socklen_t len = sizeof(client_addr);
    int client_fd = accept(g_listen_fd,
                           reinterpret_cast<struct sockaddr *>(&client_addr),
                           &len);
    if (client_fd < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
        continue;
      // Socket likely closed during stop.
      break;
    }

    std::thread client_thread([client_fd]() { handle_client(client_fd); });
    client_thread.detach();
  }
}

} // namespace

bool ftp_server_start() {
  pthread_mutex_lock(&g_state_mutex);
  if (g_running.load()) {
    pthread_mutex_unlock(&g_state_mutex);
    return true;
  }

  g_should_stop.store(false);

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    pthread_mutex_unlock(&g_state_mutex);
    LOG_ERROR("FTP server: socket() failed");
    return false;
  }

  int opt = 1;
  (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(kFtpPort);

  if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
    LOG_ERROR("FTP server: bind() on port %d failed", kFtpPort);
    close(fd);
    pthread_mutex_unlock(&g_state_mutex);
    return false;
  }

  if (listen(fd, kFtpBacklog) < 0) {
    LOG_ERROR("FTP server: listen() failed");
    close(fd);
    pthread_mutex_unlock(&g_state_mutex);
    return false;
  }

  g_listen_fd = fd;
  g_running.store(true);
  g_accept_thread = std::thread(accept_loop);
  g_accept_thread.detach();

  pthread_mutex_unlock(&g_state_mutex);

  LOG_INFO("FTP server started on port %d", kFtpPort);
  {
    char ip_buf[40] = {};
    if (get_ip_address(ip_buf, sizeof(ip_buf)) < 0)
      std::strncpy(ip_buf, "0.0.0.0", sizeof(ip_buf) - 1);
    onion_notify(true, "notify.ftp.started", ip_buf);
  }
  return true;
}

bool ftp_server_stop() {
  pthread_mutex_lock(&g_state_mutex);
  if (!g_running.load()) {
    pthread_mutex_unlock(&g_state_mutex);
    return true;
  }

  g_should_stop.store(true);
  int fd = g_listen_fd;
  g_listen_fd = -1;
  if (fd >= 0)
    close(fd);

  g_running.store(false);
  pthread_mutex_unlock(&g_state_mutex);

  LOG_INFO("FTP server stopped");
  onion_notify(true, "notify.ftp.stopped");
  return true;
}

bool ftp_server_is_running() { return g_running.load(); }

std::string ftp_server_status_json() {
  char ip_buf[40] = {};
  if (get_ip_address(ip_buf, sizeof(ip_buf)) < 0)
    std::strncpy(ip_buf, "0.0.0.0", sizeof(ip_buf) - 1);

  cJSON *root = cJSON_CreateObject();
  cJSON_AddBoolToObject(root, "enabled", g_running.load());
  cJSON_AddStringToObject(root, "ip", ip_buf);
  cJSON_AddNumberToObject(root, "port", kFtpPort);
  char *raw = cJSON_PrintUnformatted(root);
  std::string result(raw ? raw : "");
  if (raw)
    cJSON_free(raw);
  cJSON_Delete(root);
  return result;
}

void ftp_server_apply_settings() {
  extern onion::SettingsStore g_settings;
  bool enabled = g_settings.snapshot().ftp_server_enabled;
  if (enabled && !g_running.load()) {
    if (!ftp_server_start())
      onion_notify(true, "notify.ftp.start_failed");
  } else if (!enabled && g_running.load()) {
    ftp_server_stop();
  }
}
