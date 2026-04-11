/**
 * blizorukost — session.cpp
 *
 * Three-tier piece prioritisation strategy adapted from Tribler (TU Delft):
 *   TIER 1 (pieces 0-19 ahead)  — set_piece_deadline(), time-critical mode
 *   TIER 2 (pieces 20-59 ahead) — high priority, rarest-first allowed
 *   TIER 3 (60+ pieces ahead)   — default_priority, swarm health
 *
 * Reference: https://github.com/Tribler/tribler (MIT)
 *            https://libtorrent.org/streaming.html  (BSD)
 *
 * Improvements over v0.2:
 *   - Fast-start: primes first HIGH_PRIORITY_PIECES pieces immediately after
 *     metadata resolve, then waits up to FAST_START_TIMEOUT_MS for piece 0
 *     so the HTTP client never blocks on first read.
 *   - Seek cancellation: seek_generation counter lets serve_range() abort
 *     instantly when a new Range request arrives for the same stream.
 *   - Condition variable for metadata signalling (no polling delay).
 *   - Cross-platform: all sockets abstracted through compat.hpp.
 */

#include "session.hpp"

#include <libtorrent/settings_pack.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/extensions/ut_pex.hpp>
#include <libtorrent/extensions/ut_metadata.hpp>
#include <libtorrent/extensions/smart_ban.hpp>

#include <algorithm>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <random>
#include <sstream>

namespace bliz {
namespace fs = std::filesystem;

/* ── Lightweight logger ───────────────────────────────────────────────── */
static void bliz_log(const char* fmt, ...) {
    using clock = std::chrono::system_clock;
    auto now    = clock::now();
    auto ms     = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()).count() % 1000;
    auto t      = clock::to_time_t(now);
    struct tm tm_buf;
#ifdef _WIN32
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf);
#endif
    char timebuf[32];
    std::strftime(timebuf, sizeof(timebuf), "%H:%M:%S", &tm_buf);
    fprintf(stderr, "[bliz %s.%03lld] ", timebuf, (long long)ms);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
    fflush(stderr);
}
#define BLIZ_LOG(...) bliz_log(__VA_ARGS__)

/* ════════════════════════════════════════════════════════════════════════
 *  Helpers
 * ════════════════════════════════════════════════════════════════════════ */

static std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    size_t b = s.find_last_not_of(" \t\r\n");
    return (a == std::string::npos) ? "" : s.substr(a, b - a + 1);
}

std::string BlizSessionImpl::detect_mime(const std::string& name) {
    auto dot = name.rfind('.');
    if (dot == std::string::npos) return "application/octet-stream";
    std::string ext = name.substr(dot + 1);
    for (auto& c : ext) c = (char)tolower((unsigned char)c);
    if (ext == "flac") return "audio/flac";
    if (ext == "mp3")  return "audio/mpeg";
    if (ext == "ogg" || ext == "oga") return "audio/ogg";
    if (ext == "m4a")  return "audio/mp4";
    if (ext == "aac")  return "audio/aac";
    if (ext == "wav")  return "audio/wav";
    if (ext == "opus") return "audio/ogg";
    if (ext == "mp4")  return "video/mp4";
    if (ext == "mkv")  return "video/x-matroska";
    return "application/octet-stream";
}

std::string BlizSessionImpl::generate_token() {
    using namespace std::chrono;
    uint64_t ms = (uint64_t)duration_cast<milliseconds>(
        steady_clock::now().time_since_epoch()).count();
    uint32_t ctr = token_counter_.fetch_add(1, std::memory_order_relaxed);
    char buf[48];
    snprintf(buf, sizeof(buf), "%llx-%x",
             (unsigned long long)ms, (unsigned)ctr);
    return buf;
}

/* ── HTTP request parsing ─────────────────────────────────────────────── */

struct ParsedRequest {
    std::string method;
    std::string path;        /* e.g. /stream/abc123 */
    int64_t     range_start = 0;
    int64_t     range_end   = -1; /* -1 means "to end"  */
    bool        has_range   = false;
    bool        keep_alive  = true;
    bool        valid       = false;
};

static ParsedRequest read_http_request(sock_t sock) {
    std::string raw;
    raw.reserve(512);
    char ch;
    struct pollfd pfd;
    pfd.fd      = sock;
    pfd.events  = POLLIN;
    pfd.revents = 0;

    while (raw.size() < 8192) {
        int ready = bliz_poll(&pfd, 1, HTTP_RECV_TIMEOUT_MS);
        if (ready <= 0) return {};
        ssize_t n = recv(sock, &ch, 1, 0);
        if (n <= 0) return {};
        raw += ch;
        size_t len = raw.size();
        if (len >= 4 &&
            raw[len-4]=='\r' && raw[len-3]=='\n' &&
            raw[len-2]=='\r' && raw[len-1]=='\n') break;
    }

    ParsedRequest req;
    std::istringstream ss(raw);
    std::string line;

    if (!std::getline(ss, line)) return {};
    if (!line.empty() && line.back() == '\r') line.pop_back();
    {
        std::istringstream rl(line);
        std::string ver;
        rl >> req.method >> req.path >> ver;
        req.keep_alive = (ver.find("1.1") != std::string::npos);
    }

    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) break;
        auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = trim(line.substr(0, colon));
        std::string val = trim(line.substr(colon + 1));

        for (auto& c : key) c = (char)tolower((unsigned char)c);

        if (key == "range") {
            req.has_range = true;
            const char* p = val.c_str();
            if (strncmp(p, "bytes=", 6) == 0) p += 6;
            char* dash = (char*)strchr(p, '-');
            if (dash) {
                req.range_start = (int64_t)strtoll(p, nullptr, 10);
                if (*(dash + 1)) req.range_end = (int64_t)strtoll(dash + 1, nullptr, 10);
            }
        } else if (key == "connection") {
            for (auto& c : val) c = (char)tolower((unsigned char)c);
            if (val.find("close") != std::string::npos) req.keep_alive = false;
        }
    }

    req.valid = !req.method.empty() && !req.path.empty();
    return req;
}

static std::string extract_token(const std::string& path) {
    const std::string prefix = "/stream/";
    if (path.rfind(prefix, 0) == 0)
        return path.substr(prefix.size());
    return {};
}

/* ════════════════════════════════════════════════════════════════════════
 *  Constructor / destructor
 * ════════════════════════════════════════════════════════════════════════ */

BlizSessionImpl::BlizSessionImpl(const BlizConfig& cfg)
    /* Copy storage_path into an owned std::string FIRST so we don't hold
       a raw pointer into Rust memory that will be dropped after this call
       returns.  cfg_.storage_path is never read after construction. */
    : storage_path_(cfg.storage_path ? cfg.storage_path : ""),
      cfg_(cfg),
      session_(lt::settings_pack{})
{
    cfg_.storage_path = nullptr; /* intentionally nulled — use storage_path_ */

    winsock_init();

    if (cfg_.cache_max_bytes == 0) cfg_.cache_max_bytes = 50ULL * 1024 * 1024 * 1024;
    if (cfg_.cache_ttl_secs  == 0) cfg_.cache_ttl_secs  = 3600;

    fs::create_directories(storage_path_);

    init_session();

    alert_thread_ = std::thread([this]{ alert_loop(); });
    http_thread_  = std::thread([this]{ http_server_loop(); });

    /* give server a moment to bind */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    BLIZ_LOG("session created — storage=%s http_port=%d",
             storage_path_.c_str(), (int)http_port_);
}

BlizSessionImpl::~BlizSessionImpl() {
    running_.store(false);
    session_.abort();

    if (server_fd_ != kInvalidSock) {
        shutdown_sock(server_fd_);
        close_sock(server_fd_);
        server_fd_ = kInvalidSock;
    }

    if (alert_thread_.joinable()) alert_thread_.join();
    if (http_thread_.joinable())  http_thread_.join();

    {
        std::lock_guard<std::mutex> lock(streams_mutex_);
        for (auto& [token, st] : streams_) {
            st->stop_flag.store(true);
            if (st->priority_thread.joinable()) st->priority_thread.join();
        }
    }

    winsock_cleanup();
}

/* ════════════════════════════════════════════════════════════════════════
 *  libtorrent session init
 * ════════════════════════════════════════════════════════════════════════ */

void BlizSessionImpl::init_session() {
    lt::settings_pack sp;

    /* Prioritise partial pieces: finish already-started pieces first to
       reduce buffer fragmentation. Adapted from Tribler's approach. */
    sp.set_bool(lt::settings_pack::prioritize_partial_pieces, true);
    sp.set_bool(lt::settings_pack::strict_end_game_mode,      false);

    /* Piece request / timeout tuned for real-time playback */
    sp.set_int(lt::settings_pack::piece_timeout,          4);
    sp.set_int(lt::settings_pack::request_timeout,        10);
    sp.set_int(lt::settings_pack::whole_pieces_threshold,  2);

    /* Aggressive peer connections for faster swarm discovery */
    sp.set_int(lt::settings_pack::connection_speed,      500);
    sp.set_int(lt::settings_pack::peer_connect_timeout,    4);
    sp.set_int(lt::settings_pack::num_want,              200);

    /* Upload slots — Give-to-Get: maintain unchoke reciprocity */
    sp.set_int(lt::settings_pack::unchoke_slots_limit, 8);

    /* Read-ahead line size: 32 pieces (libtorrent 2.x) */
    sp.set_int(lt::settings_pack::read_cache_line_size, 32);

    /* DHT + LSD for peer discovery */
    sp.set_bool(lt::settings_pack::enable_dht,   true);
    sp.set_bool(lt::settings_pack::enable_lsd,   true);
    sp.set_bool(lt::settings_pack::enable_upnp,  true);

    sp.set_int(lt::settings_pack::alert_mask,
               lt::alert_category::status |
               lt::alert_category::piece_progress |
               lt::alert_category::error);

    if (cfg_.listen_port > 0) {
        sp.set_str(lt::settings_pack::listen_interfaces,
                   "0.0.0.0:" + std::to_string(cfg_.listen_port) +
                   ",[::]:"   + std::to_string(cfg_.listen_port));
    }

    session_.apply_settings(sp);

    session_.add_extension(&lt::create_ut_pex_plugin);
    session_.add_extension(&lt::create_ut_metadata_plugin);
    session_.add_extension(&lt::create_smart_ban_plugin);
}

/* ════════════════════════════════════════════════════════════════════════
 *  Alert processing thread
 * ════════════════════════════════════════════════════════════════════════ */

void BlizSessionImpl::alert_loop() {
    while (running_.load()) {
        session_.wait_for_alert(std::chrono::milliseconds(200));

        std::vector<lt::alert*> alerts;
        session_.pop_alerts(&alerts);

        for (auto* a : alerts) {
            if (auto* p = lt::alert_cast<lt::read_piece_alert>(a))
                on_read_piece(p);
            else if (auto* p = lt::alert_cast<lt::metadata_received_alert>(a))
                on_metadata_received(p);
            else if (auto* p = lt::alert_cast<lt::metadata_failed_alert>(a))
                on_metadata_failed(p);
        }
    }
}

void BlizSessionImpl::on_read_piece(lt::read_piece_alert* rpa) {
    if (rpa->error) {
        BLIZ_LOG("read_piece ERROR piece=%d: %s",
                 (int)rpa->piece, rpa->error.message().c_str());
        return;
    }

    std::vector<char> data(rpa->buffer.get(),
                           rpa->buffer.get() + rpa->size);
    int piece_idx = (int)rpa->piece;

    std::lock_guard<std::mutex> slock(streams_mutex_);
    for (auto& [token, st] : streams_) {
        if (st->handle != rpa->handle) continue;

        std::lock_guard<std::mutex> wlock(st->waiters_mutex);
        auto it = st->waiters.find(piece_idx);
        if (it == st->waiters.end()) break;

        for (auto& promise : it->second.promises) {
            try { promise.set_value(data); }
            catch (const std::future_error&) {}
        }
        st->waiters.erase(it);
        break;
    }
}

void BlizSessionImpl::on_metadata_received(lt::metadata_received_alert* a) {
    BLIZ_LOG("metadata_received alert — scanning streams for match");
    std::lock_guard<std::mutex> lock(streams_mutex_);
    int matched = 0;
    for (auto& [token, st] : streams_) {
        if (st->handle == a->handle) {
            {
                std::lock_guard<std::mutex> ml(st->metadata_mtx);
                st->metadata_ready.store(true);
            }
            st->metadata_cv.notify_all();
            BLIZ_LOG("metadata_received -> token=%s notified", token.c_str());
            ++matched;
        }
    }
    if (matched == 0) {
        BLIZ_LOG("metadata_received — no matching stream found (handle registered late?)");
    }
}

void BlizSessionImpl::on_metadata_failed(lt::metadata_failed_alert* a) {
    BLIZ_LOG("metadata_failed alert");
    std::lock_guard<std::mutex> lock(streams_mutex_);
    for (auto& [token, st] : streams_) {
        if (st->handle == a->handle) {
            {
                std::lock_guard<std::mutex> ml(st->metadata_mtx);
                st->metadata_failed.store(true);
            }
            st->metadata_cv.notify_all();
            BLIZ_LOG("metadata_failed -> token=%s notified", token.c_str());
        }
    }
}

/* ════════════════════════════════════════════════════════════════════════
 *  HTTP server
 * ════════════════════════════════════════════════════════════════════════ */

void BlizSessionImpl::http_server_loop() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == kInvalidSock) return;

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR,
               (const char*)&opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;

    if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close_sock(server_fd_); server_fd_ = kInvalidSock; return;
    }

    socklen_t len = sizeof(addr);
    getsockname(server_fd_, (struct sockaddr*)&addr, &len);
    http_port_ = ntohs(addr.sin_port);

    listen(server_fd_, 64);

    while (running_.load()) {
        struct pollfd pfd;
        pfd.fd      = server_fd_;
        pfd.events  = POLLIN;
        pfd.revents = 0;
        if (bliz_poll(&pfd, 1, 500) <= 0) continue;

        sock_t client = accept(server_fd_, nullptr, nullptr);
        if (client == kInvalidSock) continue;

        std::thread([this, client]() {
            handle_http_connection(client);
            close_sock(client);
        }).detach();
    }
}

void BlizSessionImpl::handle_http_connection(sock_t sock) {
    /* Disable Nagle: first audio byte reaches the player faster */
    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
               (const char*)&flag, sizeof(flag));

    bool keep_alive = true;
    while (keep_alive && running_.load()) {
        ParsedRequest req = read_http_request(sock);
        if (!req.valid) break;

        if (req.method == "OPTIONS") {
            const char* cors =
                "HTTP/1.1 204 No Content\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Access-Control-Allow-Methods: GET, HEAD\r\n"
                "Access-Control-Allow-Headers: Range\r\n"
                "Connection: keep-alive\r\n\r\n";
            send(sock, cors, (int)strlen(cors), MSG_NOSIGNAL);
            continue;
        }

        std::string token = extract_token(req.path);
        if (token.empty()) {
            const char* r404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            send(sock, r404, (int)strlen(r404), MSG_NOSIGNAL);
            break;
        }

        std::shared_ptr<StreamState> st;
        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            auto it = streams_.find(token);
            if (it != streams_.end()) st = it->second;
        }

        if (!st || !st->ti) {
            const char* r404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            send(sock, r404, (int)strlen(r404), MSG_NOSIGNAL);
            break;
        }

        /* ── Seek: bump generation to cancel any in-flight serve_range ── */
        uint64_t gen = st->seek_generation.load(std::memory_order_acquire);
        if (req.has_range) {
            gen = st->seek_generation.fetch_add(1, std::memory_order_acq_rel) + 1;
        }

        int64_t file_size = st->file_size;
        int64_t rstart    = req.has_range ? req.range_start : 0;
        int64_t rend      = (req.has_range && req.range_end >= 0)
                            ? req.range_end : file_size - 1;
        rstart = std::clamp(rstart, (int64_t)0, file_size - 1);
        rend   = std::clamp(rend,   rstart,     file_size - 1);
        int64_t content_len = rend - rstart + 1;

        st->playback_byte.store(rstart, std::memory_order_relaxed);

        char hdr[640];
        if (req.has_range) {
            snprintf(hdr, sizeof(hdr),
                "HTTP/1.1 206 Partial Content\r\n"
                "Content-Type: %s\r\n"
                "Content-Length: %lld\r\n"
                "Content-Range: bytes %lld-%lld/%lld\r\n"
                "Accept-Ranges: bytes\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Connection: %s\r\n\r\n",
                st->mime_type.c_str(),
                (long long)content_len,
                (long long)rstart, (long long)rend, (long long)file_size,
                req.keep_alive ? "keep-alive" : "close");
        } else {
            snprintf(hdr, sizeof(hdr),
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: %s\r\n"
                "Content-Length: %lld\r\n"
                "Accept-Ranges: bytes\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Connection: %s\r\n\r\n",
                st->mime_type.c_str(),
                (long long)file_size,
                req.keep_alive ? "keep-alive" : "close");
        }
        if (send(sock, hdr, (int)strlen(hdr), MSG_NOSIGNAL) < 0) break;

        if (req.method == "HEAD") continue;

        bool ok = serve_range(sock, *st, rstart, rend, gen);
        keep_alive = ok && req.keep_alive;
    }
}

bool BlizSessionImpl::serve_range(sock_t sock, StreamState& stream,
                                  int64_t start, int64_t end,
                                  uint64_t gen) {
    int piece_len = stream.ti->piece_length();
    int64_t pos   = start;

    while (pos <= end && running_.load()) {
        if (stream.stop_flag.load()) return false;

        /* abort if a newer seek has arrived for this stream */
        if (stream.seek_generation.load(std::memory_order_acquire) != gen)
            return false;

        int64_t abs_pos   = stream.file_offset + pos;
        int piece_idx     = (int)(abs_pos / piece_len);
        int piece_off     = (int)(abs_pos % piece_len);

        auto piece_data = wait_for_piece(stream, piece_idx, gen);
        if (piece_data.empty()) return false;

        int64_t available     = (int64_t)piece_data.size() - piece_off;
        int64_t bytes_to_send = std::min(available, end - pos + 1);
        if (bytes_to_send <= 0) break;

        const char* buf = piece_data.data() + piece_off;
        int64_t sent = 0;
        while (sent < bytes_to_send) {
            ssize_t n = send(sock, buf + sent,
                             (size_t)(bytes_to_send - sent), MSG_NOSIGNAL);
            if (n <= 0) return false;
            sent += n;
        }

        pos += bytes_to_send;
    }
    return true;
}

/* ════════════════════════════════════════════════════════════════════════
 *  Piece read synchronisation
 * ════════════════════════════════════════════════════════════════════════ */

std::vector<char> BlizSessionImpl::wait_for_piece(StreamState& stream,
                                                   int piece_idx,
                                                   uint64_t seek_gen) {
    /* Check if torrent is paused — if so, log a warning immediately */
    if (stream.handle.flags() & lt::torrent_flags::paused) {
        BLIZ_LOG("wait_for_piece(%d) WARNING: torrent is PAUSED — piece will never arrive",
                 piece_idx);
    }

    /* If we already have this piece, read it directly */
    if (stream.handle.have_piece(lt::piece_index_t(piece_idx))) {
        std::future<std::vector<char>> future;
        {
            std::lock_guard<std::mutex> lock(stream.waiters_mutex);
            std::promise<std::vector<char>> promise;
            future = promise.get_future();
            stream.waiters[piece_idx].promises.push_back(std::move(promise));
        }
        stream.handle.read_piece(lt::piece_index_t(piece_idx));

        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(2000);
        while (std::chrono::steady_clock::now() < deadline) {
            auto status = future.wait_for(std::chrono::milliseconds(50));
            if (status == std::future_status::ready) {
                try { return future.get(); } catch (...) { return {}; }
            }
        }
        std::lock_guard<std::mutex> lock(stream.waiters_mutex);
        stream.waiters.erase(piece_idx);
        return {};
    }

    /* Request download with urgent deadline */
    std::future<std::vector<char>> future;
    {
        std::lock_guard<std::mutex> lock(stream.waiters_mutex);
        std::promise<std::vector<char>> promise;
        future = promise.get_future();
        stream.waiters[piece_idx].promises.push_back(std::move(promise));
    }

    stream.handle.set_piece_deadline(
        lt::piece_index_t(piece_idx),
        0,
        lt::torrent_handle::alert_when_available
    );

    /* Poll with short intervals so we can detect seek cancellation */
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(PIECE_TIMEOUT_MS);

    while (std::chrono::steady_clock::now() < deadline) {
        /* abort if a newer seek arrived */
        if (stream.seek_generation.load(std::memory_order_acquire) != seek_gen) {
            std::lock_guard<std::mutex> lock(stream.waiters_mutex);
            stream.waiters.erase(piece_idx);
            return {};
        }

        auto status = future.wait_for(std::chrono::milliseconds(100));
        if (status == std::future_status::ready) {
            try { return future.get(); } catch (...) { return {}; }
        }
    }

    BLIZ_LOG("wait_for_piece(%d) TIMEOUT after %dms — paused=%s",
             piece_idx, PIECE_TIMEOUT_MS,
             (stream.handle.flags() & lt::torrent_flags::paused) ? "YES" : "no");
    std::lock_guard<std::mutex> lock(stream.waiters_mutex);
    stream.waiters.erase(piece_idx);
    return {};
}

/* ════════════════════════════════════════════════════════════════════════
 *  Three-tier piece priority worker
 *  Adapted from Tribler streaming research (TU Delft, MIT licence)
 * ════════════════════════════════════════════════════════════════════════ */

void BlizSessionImpl::run_priority_worker(std::shared_ptr<StreamState> stream) {
    while (!stream->stop_flag.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(PRIORITY_TICK_MS));
        if (stream->stop_flag.load()) break;

        auto ti = stream->ti;
        if (!ti || !stream->handle.is_valid()) continue;

        int piece_len = ti->piece_length();
        int64_t play_byte = stream->playback_byte.load(std::memory_order_relaxed);
        int64_t abs_pos   = stream->file_offset + play_byte;
        int play_piece    = (int)(abs_pos / piece_len);
        play_piece = std::clamp(play_piece, stream->first_piece, stream->last_piece);

        /* ── TIER 1: hard deadline window (0 – HIGH_PRIORITY_PIECES)
           Pieces the player needs in the next few seconds.
           Spacing of 150 ms emulates a typical compressed audio bitrate.  ── */
        for (int i = play_piece;
             i < std::min(play_piece + HIGH_PRIORITY_PIECES, stream->last_piece + 1);
             ++i) {
            if (!stream->handle.have_piece(lt::piece_index_t(i))) {
                int deadline_ms = (i - play_piece) * 150;
                stream->handle.set_piece_deadline(
                    lt::piece_index_t(i), deadline_ms);
            }
        }

        /* ── Set priority on all file pieces ────────────────────────── */
        for (int i = stream->first_piece; i <= stream->last_piece; ++i) {
            lt::download_priority_t prio;

            if (i < play_piece) {
                prio = lt::low_priority;

            } else if (i < play_piece + HIGH_PRIORITY_PIECES) {
                /* TIER 1: handled by set_piece_deadline above */
                prio = lt::top_priority;

            } else if (i < play_piece + MID_PRIORITY_PIECES) {
                /* TIER 2: rarest-first allowed, high priority */
                prio = lt::download_priority_t{6};

            } else {
                /* TIER 3: normal rarest-first — keeps us a good peer */
                prio = lt::default_priority;
            }

            stream->handle.piece_priority(lt::piece_index_t(i), prio);
        }
    }
}

/* ════════════════════════════════════════════════════════════════════════
 *  add_torrent_params helper
 * ════════════════════════════════════════════════════════════════════════ */

lt::add_torrent_params BlizSessionImpl::make_atp(const char*    magnet,
                                                  const uint8_t* data,
                                                  size_t         len) {
    lt::add_torrent_params atp;

    if (data && len > 0) {
        lt::error_code ec;
        auto ti = std::make_shared<lt::torrent_info>(
            (const char*)data, (int)len, ec);
        if (ec) throw std::runtime_error(ec.message());
        atp.ti = ti;
    } else if (magnet && *magnet) {
        lt::error_code ec;
        atp = lt::parse_magnet_uri(magnet, ec);
        if (ec) throw std::runtime_error(ec.message());
    } else {
        throw std::runtime_error("no magnet or torrent data");
    }

    atp.save_path = storage_path_;
    atp.flags |= lt::torrent_flags::sequential_download;
    atp.flags &= ~lt::torrent_flags::auto_managed;
    atp.flags &= ~lt::torrent_flags::paused;

    return atp;
}

/* ════════════════════════════════════════════════════════════════════════
 *  prepare()
 * ════════════════════════════════════════════════════════════════════════ */

BlizStreamInfo BlizSessionImpl::prepare(const char*    magnet,
                                         const uint8_t* torrent_data,
                                         size_t         torrent_len,
                                         int            file_idx,
                                         BlizProgressFn progress_fn,
                                         void*          userdata) {
    BlizStreamInfo info{};

    auto report = [&](float p, const char* s) {
        if (progress_fn) progress_fn(p, s, userdata);
    };

    BLIZ_LOG("prepare() start — file_idx=%d has_torrent_data=%s",
             file_idx, (torrent_data && torrent_len > 0) ? "yes" : "no");

    try {
        lt::add_torrent_params atp = make_atp(magnet, torrent_data, torrent_len);
        BLIZ_LOG("prepare() atp built");

        std::string token = generate_token();
        auto st = std::make_shared<StreamState>();
        st->file_idx = file_idx;

        report(0.05f, "Connecting to swarm...");
        BLIZ_LOG("prepare(%s) add_torrent...", token.c_str());
        lt::torrent_handle handle = session_.add_torrent(atp);
        st->handle = handle;
        BLIZ_LOG("prepare(%s) add_torrent done — valid=%s paused=%s",
                 token.c_str(),
                 handle.is_valid() ? "yes" : "no",
                 (handle.flags() & lt::torrent_flags::paused) ? "YES (WARNING)" : "no");

        /* Check for immediately-available metadata (torrent data provided,
           not a magnet-only add).  libtorrent does NOT fire
           metadata_received_alert when atp.ti is already set. */
        {
            auto ti_immed = handle.torrent_file();
            if (ti_immed) {
                st->ti = ti_immed;
                st->metadata_ready.store(true, std::memory_order_release);
                BLIZ_LOG("prepare(%s) metadata immediate (torrent data path)", token.c_str());
            }
        }

        /* Register the stream (handle already set above). */
        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_[token] = st;
        }

        /* ── Wait for metadata using condition variable (magnet-only path) */
        if (!st->metadata_ready.load(std::memory_order_acquire)) {
            BLIZ_LOG("prepare(%s) waiting for metadata (magnet path, timeout=%ds)...",
                     token.c_str(), METADATA_TIMEOUT_S);
            report(0.08f, "Resolving metadata...");
            std::unique_lock<std::mutex> lk(st->metadata_mtx);
            bool signalled = st->metadata_cv.wait_for(
                lk,
                std::chrono::seconds(METADATA_TIMEOUT_S),
                [&]{ return st->metadata_ready.load() || st->metadata_failed.load(); }
            );
            (void)signalled;

            /* Last-chance fallback in case alert fired before stream was registered */
            if (!st->metadata_ready.load()) {
                auto ti = handle.torrent_file();
                if (ti) { st->ti = ti; st->metadata_ready.store(true); }
            }

            if (st->metadata_failed.load())
                BLIZ_LOG("prepare(%s) metadata FAILED", token.c_str());
            else if (st->metadata_ready.load())
                BLIZ_LOG("prepare(%s) metadata received via alert", token.c_str());
            else
                BLIZ_LOG("prepare(%s) metadata TIMED OUT after %ds", token.c_str(), METADATA_TIMEOUT_S);
        }

        if (!st->metadata_ready.load()) {
            BLIZ_LOG("prepare(%s) FAIL: metadata not ready, removing torrent", token.c_str());
            session_.remove_torrent(handle);
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
            info.error = BLIZ_ERR_METADATA_TIMEOUT;
            snprintf(info.error_msg, sizeof(info.error_msg),
                     "Metadata timeout after %d s", METADATA_TIMEOUT_S);
            return info;
        }

        if (!st->ti) {
            st->ti = handle.torrent_file();
        }

        if (!st->ti) {
            BLIZ_LOG("prepare(%s) FAIL: torrent_file() null after metadata ready", token.c_str());
            session_.remove_torrent(handle);
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
            info.error = BLIZ_ERR_METADATA_TIMEOUT;
            snprintf(info.error_msg, sizeof(info.error_msg), "torrent_file() null after metadata");
            return info;
        }

        /* ── Resolve file ─────────────────────────────────────────────── */
        auto& files     = st->ti->files();
        int   num_files = files.num_files();

        BLIZ_LOG("prepare(%s) torrent has %d files, requesting file_idx=%d",
                 token.c_str(), num_files, file_idx);

        if (file_idx < 0 || file_idx >= num_files) {
            BLIZ_LOG("prepare(%s) FAIL: file_idx %d out of range", token.c_str(), file_idx);
            session_.remove_torrent(handle);
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
            info.error = BLIZ_ERR_INVALID_FILE;
            snprintf(info.error_msg, sizeof(info.error_msg),
                     "file_idx %d out of range (torrent has %d files)",
                     file_idx, num_files);
            return info;
        }

        st->file_offset = files.file_offset(lt::file_index_t(file_idx));
        st->file_size   = files.file_size(lt::file_index_t(file_idx));
        st->mime_type   = detect_mime(
            files.file_name(lt::file_index_t(file_idx)).to_string());

        int piece_len   = st->ti->piece_length();
        st->first_piece = (int)(st->file_offset / piece_len);
        st->last_piece  = (int)((st->file_offset + st->file_size - 1) / piece_len);

        BLIZ_LOG("prepare(%s) file=%s size=%lld mime=%s pieces=[%d..%d] piece_len=%d",
                 token.c_str(),
                 files.file_name(lt::file_index_t(file_idx)).to_string().c_str(),
                 (long long)st->file_size,
                 st->mime_type.c_str(),
                 st->first_piece, st->last_piece, piece_len);

        /* ── Pin file priorities ──────────────────────────────────────── */
        std::vector<lt::download_priority_t> file_prios(
            (size_t)num_files, lt::dont_download);
        file_prios[(size_t)file_idx] = lt::top_priority;
        handle.prioritize_files(file_prios);

        /* If the torrent was paused (e.g. released previously), resume it. */
        if (handle.flags() & lt::torrent_flags::paused) {
            BLIZ_LOG("prepare(%s) torrent was paused — resuming", token.c_str());
        }
        handle.resume();

        /* ── Fast-start: prime deadline on first HIGH_PRIORITY_PIECES ─── */
        report(0.60f, "Buffering...");
        int primed = 0;
        for (int i = st->first_piece;
             i < std::min(st->first_piece + HIGH_PRIORITY_PIECES, st->last_piece + 1);
             ++i) {
            handle.set_piece_deadline(lt::piece_index_t(i), 0);
            ++primed;
        }
        BLIZ_LOG("prepare(%s) fast-start: primed %d pieces, waiting for piece %d...",
                 token.c_str(), primed, st->first_piece);

        /* Wait until the very first piece is ready (up to FAST_START_TIMEOUT_MS). */
        auto fast_start_begin = std::chrono::steady_clock::now();
        auto fast_start_dl    = fast_start_begin +
                                std::chrono::milliseconds(FAST_START_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < fast_start_dl) {
            if (handle.have_piece(lt::piece_index_t(st->first_piece))) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            float elapsed_ms = std::chrono::duration<float, std::milli>(
                std::chrono::steady_clock::now() - fast_start_dl +
                std::chrono::milliseconds(FAST_START_TIMEOUT_MS)).count();
            report(0.60f + (elapsed_ms / FAST_START_TIMEOUT_MS) * 0.35f,
                   "Buffering...");
        }

        bool piece0_ready = handle.have_piece(lt::piece_index_t(st->first_piece));
        auto elapsed_fast = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - fast_start_begin).count();
        BLIZ_LOG("prepare(%s) fast-start done in %lldms — piece %d ready=%s",
                 token.c_str(), (long long)elapsed_fast,
                 st->first_piece, piece0_ready ? "YES" : "NO (continuing anyway)");

        /* ── Start priority worker ────────────────────────────────────── */
        st->priority_thread = std::thread([this, st]() {
            run_priority_worker(st);
        });

        report(0.98f, "Ready");

        /* ── Build result ─────────────────────────────────────────────── */
        snprintf(info.url, sizeof(info.url),
                 "http://127.0.0.1:%d/stream/%s",
                 (int)http_port_, token.c_str());
        snprintf(info.token,     sizeof(info.token),     "%s", token.c_str());
        snprintf(info.mime_type, sizeof(info.mime_type), "%s", st->mime_type.c_str());
        info.file_size = st->file_size;
        info.error     = BLIZ_OK;

        BLIZ_LOG("prepare(%s) SUCCESS — url=%s", token.c_str(), info.url);

    } catch (const std::exception& e) {
        BLIZ_LOG("prepare() EXCEPTION: %s", e.what());
        info.error = BLIZ_ERR_INTERNAL;
        snprintf(info.error_msg, sizeof(info.error_msg), "%s", e.what());
    }

    return info;
}

/* ════════════════════════════════════════════════════════════════════════
 *  list_files()
 * ════════════════════════════════════════════════════════════════════════ */

BlizFileList BlizSessionImpl::list_files(const char*    magnet,
                                          const uint8_t* torrent_data,
                                          size_t         torrent_len) {
    BlizFileList result{};

    try {
        lt::add_torrent_params atp = make_atp(magnet, torrent_data, torrent_len);

        auto st = std::make_shared<StreamState>();
        std::string token = "list-" + generate_token();

        lt::torrent_handle handle = session_.add_torrent(atp);
        st->handle = handle;

        /* Immediate check for torrent-data case (no alert fired) */
        {
            auto ti_immed = handle.torrent_file();
            if (ti_immed) {
                st->ti = ti_immed;
                st->metadata_ready.store(true, std::memory_order_release);
            }
        }

        if (!st->metadata_ready.load(std::memory_order_acquire)) {
            {
                std::lock_guard<std::mutex> lock(streams_mutex_);
                streams_[token] = st;
            }

            std::unique_lock<std::mutex> lk(st->metadata_mtx);
            st->metadata_cv.wait_for(
                lk,
                std::chrono::seconds(METADATA_TIMEOUT_S),
                [&]{ return st->metadata_ready.load() || st->metadata_failed.load(); }
            );

            if (!st->metadata_ready.load()) {
                auto ti = handle.torrent_file();
                if (ti) { st->ti = ti; st->metadata_ready.store(true); }
            }

            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
        }

        if (!st->metadata_ready.load()) {
            session_.remove_torrent(handle);
            result.error = BLIZ_ERR_METADATA_TIMEOUT;
            snprintf(result.error_msg, sizeof(result.error_msg), "Metadata timeout");
            return result;
        }

        if (!st->ti) st->ti = handle.torrent_file();
        session_.remove_torrent(handle);

        if (!st->ti) {
            result.error = BLIZ_ERR_INTERNAL;
            snprintf(result.error_msg, sizeof(result.error_msg), "torrent_file() null");
            return result;
        }

        auto& files   = st->ti->files();
        int   n       = files.num_files();
        auto* entries = new BlizFileEntry[(size_t)n];

        for (int i = 0; i < n; ++i) {
            std::string nm = files.file_name(lt::file_index_t(i)).to_string();
            snprintf(entries[i].name, sizeof(entries[i].name), "%s", nm.c_str());
            snprintf(entries[i].mime, sizeof(entries[i].mime), "%s",
                     detect_mime(nm).c_str());
            entries[i].size  = files.file_size(lt::file_index_t(i));
            entries[i].index = i;
        }

        result.files = entries;
        result.count = n;
        result.error = BLIZ_OK;

    } catch (const std::exception& e) {
        result.error = BLIZ_ERR_INTERNAL;
        snprintf(result.error_msg, sizeof(result.error_msg), "%s", e.what());
    }

    return result;
}

/* ════════════════════════════════════════════════════════════════════════
 *  notify_position / release_stream / evict
 * ════════════════════════════════════════════════════════════════════════ */

void BlizSessionImpl::notify_position(const std::string& token,
                                       int64_t byte_offset) {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    auto it = streams_.find(token);
    if (it != streams_.end())
        it->second->playback_byte.store(byte_offset, std::memory_order_relaxed);
}

void BlizSessionImpl::release_stream(const std::string& token) {
    BLIZ_LOG("release_stream(%s) start", token.c_str());
    std::shared_ptr<StreamState> st;
    {
        std::lock_guard<std::mutex> lock(streams_mutex_);
        auto it = streams_.find(token);
        if (it == streams_.end()) {
            BLIZ_LOG("release_stream(%s) token not found — already released?", token.c_str());
            return;
        }
        st = it->second;
        streams_.erase(it);
    }

    st->stop_flag.store(true);
    /* bump generation so any blocking serve_range/wait_for_piece exits */
    st->seek_generation.fetch_add(1, std::memory_order_release);

    BLIZ_LOG("release_stream(%s) joining priority_thread...", token.c_str());
    auto t0 = std::chrono::steady_clock::now();
    if (st->priority_thread.joinable())
        st->priority_thread.join();
    auto join_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    BLIZ_LOG("release_stream(%s) priority_thread joined in %lldms", token.c_str(), (long long)join_ms);

    if (st->handle.is_valid()) {
        st->handle.pause();
        std::lock_guard<std::mutex> lock(idle_mutex_);
        idle_torrents_.push_back({st->handle,
                                  std::chrono::steady_clock::now()});
        BLIZ_LOG("release_stream(%s) torrent paused and moved to idle cache", token.c_str());
    }
    BLIZ_LOG("release_stream(%s) done", token.c_str());
}

void BlizSessionImpl::evict() {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(idle_mutex_);

    idle_torrents_.erase(
        std::remove_if(idle_torrents_.begin(), idle_torrents_.end(),
            [&](const IdleTorrent& it) {
                auto age = std::chrono::duration_cast<std::chrono::seconds>(
                    now - it.since).count();
                if ((uint64_t)age > cfg_.cache_ttl_secs) {
                    session_.remove_torrent(it.handle);
                    return true;
                }
                return false;
            }),
        idle_torrents_.end()
    );
}

} // namespace bliz
