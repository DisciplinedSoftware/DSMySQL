#pragma once

#include <mysql.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ds_mysql/charset_name.hpp"

namespace ds_mysql {

// ===================================================================
// ssl_mode — maps to MySQL SSL_MODE_* constants.
// ===================================================================

enum class ssl_mode : unsigned int {
    disabled = SSL_MODE_DISABLED,
    preferred = SSL_MODE_PREFERRED,
    required = SSL_MODE_REQUIRED,
    verify_ca = SSL_MODE_VERIFY_CA,
    verify_identity = SSL_MODE_VERIFY_IDENTITY,
};

// ===================================================================
// connect_options — pre-connect options applied between mysql_init()
// and mysql_real_connect().
//
// Usage:
//   auto opts = connect_options{}
//       .connect_timeout(5)
//       .read_timeout(30)
//       .write_timeout(30)
//       .ssl(ssl_mode::required)
//       .charset(charset_name{"utf8mb4"});
//
//   auto db = mysql_connection::connect(host, database, creds, port, opts);
// ===================================================================

class connect_options {
public:
    connect_options() = default;

    // ---------------------------------------------------------------
    // Timeouts (seconds)
    // ---------------------------------------------------------------

    [[nodiscard]] connect_options connect_timeout(unsigned int seconds) const {
        auto copy = *this;
        copy.connect_timeout_ = seconds;
        return copy;
    }

    [[nodiscard]] connect_options read_timeout(unsigned int seconds) const {
        auto copy = *this;
        copy.read_timeout_ = seconds;
        return copy;
    }

    [[nodiscard]] connect_options write_timeout(unsigned int seconds) const {
        auto copy = *this;
        copy.write_timeout_ = seconds;
        return copy;
    }

    // ---------------------------------------------------------------
    // SSL / TLS
    // ---------------------------------------------------------------

    [[nodiscard]] connect_options ssl(ssl_mode mode) const {
        auto copy = *this;
        copy.ssl_mode_ = mode;
        return copy;
    }

    [[nodiscard]] connect_options ssl_ca(std::string path) const {
        auto copy = *this;
        copy.ssl_ca_ = std::move(path);
        return copy;
    }

    [[nodiscard]] connect_options ssl_cert(std::string path) const {
        auto copy = *this;
        copy.ssl_cert_ = std::move(path);
        return copy;
    }

    [[nodiscard]] connect_options ssl_key(std::string path) const {
        auto copy = *this;
        copy.ssl_key_ = std::move(path);
        return copy;
    }

    [[nodiscard]] connect_options ssl_capath(std::string path) const {
        auto copy = *this;
        copy.ssl_capath_ = std::move(path);
        return copy;
    }

    [[nodiscard]] connect_options ssl_cipher(std::string cipher) const {
        auto copy = *this;
        copy.ssl_cipher_ = std::move(cipher);
        return copy;
    }

    [[nodiscard]] connect_options tls_version(std::string version) const {
        auto copy = *this;
        copy.tls_version_ = std::move(version);
        return copy;
    }

    [[nodiscard]] connect_options tls_ciphersuites(std::string suites) const {
        auto copy = *this;
        copy.tls_ciphersuites_ = std::move(suites);
        return copy;
    }

    // ---------------------------------------------------------------
    // Connection behaviour
    // ---------------------------------------------------------------

    [[nodiscard]] connect_options charset(charset_name cs) const {
        auto copy = *this;
        copy.charset_ = std::move(cs);
        return copy;
    }

    [[nodiscard]] connect_options compress(bool enable = true) const {
        auto copy = *this;
        copy.compress_ = enable;
        return copy;
    }

    [[nodiscard]] connect_options reconnect(bool enable) const {
        auto copy = *this;
        copy.reconnect_ = enable;
        return copy;
    }

    [[nodiscard]] connect_options local_infile(bool enable) const {
        auto copy = *this;
        copy.local_infile_ = enable;
        return copy;
    }

    [[nodiscard]] connect_options init_command(std::string sql) const {
        auto copy = *this;
        copy.init_command_ = std::move(sql);
        return copy;
    }

    [[nodiscard]] connect_options default_auth(std::string plugin) const {
        auto copy = *this;
        copy.default_auth_ = std::move(plugin);
        return copy;
    }

    [[nodiscard]] connect_options compression_algorithms(std::string algos) const {
        auto copy = *this;
        copy.compression_algorithms_ = std::move(algos);
        return copy;
    }

    [[nodiscard]] connect_options zstd_compression_level(unsigned int level) const {
        auto copy = *this;
        copy.zstd_compression_level_ = level;
        return copy;
    }

    [[nodiscard]] connect_options max_allowed_packet(unsigned long bytes) const {
        auto copy = *this;
        copy.max_allowed_packet_ = bytes;
        return copy;
    }

    [[nodiscard]] connect_options retry_count(unsigned int count) const {
        auto copy = *this;
        copy.retry_count_ = count;
        return copy;
    }

    [[nodiscard]] connect_options bind_address(std::string addr) const {
        auto copy = *this;
        copy.bind_address_ = std::move(addr);
        return copy;
    }

    // ---------------------------------------------------------------
    // Connection attributes (key-value metadata sent to server)
    // ---------------------------------------------------------------

    [[nodiscard]] connect_options attr(std::string key, std::string value) const {
        auto copy = *this;
        copy.attrs_.emplace_back(std::move(key), std::move(value));
        return copy;
    }

    // Apply all configured options to a MYSQL* handle.
    // Called between mysql_init() and mysql_real_connect().
    [[nodiscard]] std::expected<void, std::string> apply(MYSQL* conn) const {
        auto set_uint = [&](mysql_option opt, unsigned int val) -> std::expected<void, std::string> {
            if (mysql_options(conn, opt, &val) != 0) {
                return std::unexpected(std::string(mysql_error(conn)));
            }
            return {};
        };
        auto set_ulong = [&](mysql_option opt, unsigned long val) -> std::expected<void, std::string> {
            if (mysql_options(conn, opt, &val) != 0) {
                return std::unexpected(std::string(mysql_error(conn)));
            }
            return {};
        };
        auto set_bool = [&](mysql_option opt, bool val) -> std::expected<void, std::string> {
            if (mysql_options(conn, opt, &val) != 0) {
                return std::unexpected(std::string(mysql_error(conn)));
            }
            return {};
        };
        auto set_str = [&](mysql_option opt, std::string const& val) -> std::expected<void, std::string> {
            if (mysql_options(conn, opt, val.c_str()) != 0) {
                return std::unexpected(std::string(mysql_error(conn)));
            }
            return {};
        };
        auto set_null = [&](mysql_option opt) -> std::expected<void, std::string> {
            if (mysql_options(conn, opt, nullptr) != 0) {
                return std::unexpected(std::string(mysql_error(conn)));
            }
            return {};
        };

        // Timeouts
        if (connect_timeout_) {
            auto r = set_uint(MYSQL_OPT_CONNECT_TIMEOUT, *connect_timeout_);
            if (!r)
                return r;
        }
        if (read_timeout_) {
            auto r = set_uint(MYSQL_OPT_READ_TIMEOUT, *read_timeout_);
            if (!r)
                return r;
        }
        if (write_timeout_) {
            auto r = set_uint(MYSQL_OPT_WRITE_TIMEOUT, *write_timeout_);
            if (!r)
                return r;
        }

        // SSL/TLS
        if (ssl_mode_) {
            auto mode = static_cast<unsigned int>(*ssl_mode_);
            auto r = set_uint(MYSQL_OPT_SSL_MODE, mode);
            if (!r)
                return r;
        }
        if (ssl_ca_) {
            auto r = set_str(MYSQL_OPT_SSL_CA, *ssl_ca_);
            if (!r)
                return r;
        }
        if (ssl_cert_) {
            auto r = set_str(MYSQL_OPT_SSL_CERT, *ssl_cert_);
            if (!r)
                return r;
        }
        if (ssl_key_) {
            auto r = set_str(MYSQL_OPT_SSL_KEY, *ssl_key_);
            if (!r)
                return r;
        }
        if (ssl_capath_) {
            auto r = set_str(MYSQL_OPT_SSL_CAPATH, *ssl_capath_);
            if (!r)
                return r;
        }
        if (ssl_cipher_) {
            auto r = set_str(MYSQL_OPT_SSL_CIPHER, *ssl_cipher_);
            if (!r)
                return r;
        }
        if (tls_version_) {
            auto r = set_str(MYSQL_OPT_TLS_VERSION, *tls_version_);
            if (!r)
                return r;
        }
        if (tls_ciphersuites_) {
            auto r = set_str(MYSQL_OPT_TLS_CIPHERSUITES, *tls_ciphersuites_);
            if (!r)
                return r;
        }

        // Connection behaviour
        if (charset_) {
            auto r = set_str(MYSQL_SET_CHARSET_NAME, charset_->to_string());
            if (!r)
                return r;
        }
        if (compress_ && *compress_) {
            auto r = set_null(MYSQL_OPT_COMPRESS);
            if (!r)
                return r;
        }
        if (reconnect_) {
            auto r = set_bool(MYSQL_OPT_RECONNECT, *reconnect_);
            if (!r)
                return r;
        }
        if (local_infile_) {
            unsigned int val = *local_infile_ ? 1 : 0;
            auto r = set_uint(MYSQL_OPT_LOCAL_INFILE, val);
            if (!r)
                return r;
        }
        if (init_command_) {
            auto r = set_str(MYSQL_INIT_COMMAND, *init_command_);
            if (!r)
                return r;
        }
        if (default_auth_) {
            auto r = set_str(MYSQL_DEFAULT_AUTH, *default_auth_);
            if (!r)
                return r;
        }
        if (compression_algorithms_) {
            auto r = set_str(MYSQL_OPT_COMPRESSION_ALGORITHMS, *compression_algorithms_);
            if (!r)
                return r;
        }
        if (zstd_compression_level_) {
            auto r = set_uint(MYSQL_OPT_ZSTD_COMPRESSION_LEVEL, *zstd_compression_level_);
            if (!r)
                return r;
        }
        if (max_allowed_packet_) {
            auto r = set_ulong(MYSQL_OPT_MAX_ALLOWED_PACKET, *max_allowed_packet_);
            if (!r)
                return r;
        }
        if (retry_count_) {
            auto r = set_uint(MYSQL_OPT_RETRY_COUNT, *retry_count_);
            if (!r)
                return r;
        }
        if (bind_address_) {
            auto r = set_str(MYSQL_OPT_BIND, *bind_address_);
            if (!r)
                return r;
        }

        // Connection attributes
        for (auto const& [key, value] : attrs_) {
            if (mysql_options4(conn, MYSQL_OPT_CONNECT_ATTR_ADD, key.c_str(), value.c_str()) != 0) {
                return std::unexpected(std::string(mysql_error(conn)));
            }
        }

        return {};
    }

private:
    // Timeouts
    std::optional<unsigned int> connect_timeout_;
    std::optional<unsigned int> read_timeout_;
    std::optional<unsigned int> write_timeout_;

    // SSL/TLS
    std::optional<ssl_mode> ssl_mode_;
    std::optional<std::string> ssl_ca_;
    std::optional<std::string> ssl_cert_;
    std::optional<std::string> ssl_key_;
    std::optional<std::string> ssl_capath_;
    std::optional<std::string> ssl_cipher_;
    std::optional<std::string> tls_version_;
    std::optional<std::string> tls_ciphersuites_;

    // Connection behaviour
    std::optional<charset_name> charset_;
    std::optional<bool> compress_;
    std::optional<bool> reconnect_;
    std::optional<bool> local_infile_;
    std::optional<std::string> init_command_;
    std::optional<std::string> default_auth_;
    std::optional<std::string> compression_algorithms_;
    std::optional<unsigned int> zstd_compression_level_;
    std::optional<unsigned long> max_allowed_packet_;
    std::optional<unsigned int> retry_count_;
    std::optional<std::string> bind_address_;

    // Connection attributes
    std::vector<std::pair<std::string, std::string>> attrs_;
};

}  // namespace ds_mysql
