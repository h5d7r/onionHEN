/* Copyright (C) 2025 OnionHEN / LightningMods
 *
 * Built-in anonymous FTP server on port 2121 for the util daemon.
 */

#pragma once

#include <string>

/** Start listening on port 2121. Idempotent if already running. */
bool ftp_server_start();

/** Stop listening and disconnect clients. Idempotent if not running. */
bool ftp_server_stop();

/** Whether the server is currently running. */
bool ftp_server_is_running();

/** JSON status: {"enabled": bool, "ip": string, "port": int}. */
std::string ftp_server_status_json();

/** Apply the current settings.ftp_server_enabled value. */
void ftp_server_apply_settings();
