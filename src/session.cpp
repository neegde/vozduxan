/**
 * vozduxan — session.cpp
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
 *   - seek_generation aborts serve_range only when the stream is released;
 *     parallel Range requests (browser sniff + read) must not cancel each other.
 *   - Condition variable for metadata signalling (no polling delay).
 *   - Cross-platform: all sockets abstracted through compat.hpp.
 */

#include "session.hpp"

#include <libtorrent/config.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/bdecode.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/extensions/ut_pex.hpp>
#include <libtorrent/extensions/ut_metadata.hpp>
#include <libtorrent/extensions/smart_ban.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>

namespace vozduxan {
namespace fs = std::filesystem;

/* Well-known open trackers appended to every torrent so thin-seeded
   magnets can still find peers via UDP announces.                    */
static const std::vector<std::string> OPEN_TRACKERS = {
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://explodie.org:6969/announce",
};

namespace {

/** Lowercase 40-char hex btih in `urn:btih:` for consistent peer/torrent id matching. */
std::string normalize_magnet_btih_hex(std::string m) {
    const char* key = "urn:btih:";
    size_t      pos = 0;
    while ((pos = m.find(key, pos)) != std::string::npos) {
        size_t h = pos + std::strlen(key);
        size_t e = h;
        while (e < m.size() && std::isxdigit(static_cast<unsigned char>(m[e]))) ++e;
        if (e - h == 40) {
            for (size_t i = h; i < e; ++i) {
                m[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(m[i])));
            }
        }
        pos = e;
    }
    return m;
}

}  // namespace

/* ── Structured logger — member function ────────────────────────────────
   If log_fn_ is set, only the callback runs (in-app console). Otherwise stderr. */
void VozduxanSessionImpl::log(const char* fmt, ...) {
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

    char body[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof(body), fmt, ap);
    va_end(ap);

    if (log_fn_) {
        char full[1088];
        snprintf(full, sizeof(full), "[vozduxan %s.%03lld] %s", timebuf, (long long)ms, body);
        log_fn_(full, log_userdata_);
    } else {
        fprintf(stderr, "[vozduxan %s.%03lld] %s\n", timebuf, (long long)ms, body);
        fflush(stderr);
    }
}
#define VOZDUXAN_LOG(...) this->log(__VA_ARGS__)

/* ════════════════════════════════════════════════════════════════════════
 *  Helpers
 * ════════════════════════════════════════════════════════════════════════ */

static std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    size_t b = s.find_last_not_of(" \t\r\n");
    return (a == std::string::npos) ? "" : s.substr(a, b - a + 1);
}

std::string VozduxanSessionImpl::detect_mime(const std::string& name) {
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

std::string VozduxanSessionImpl::generate_token() {
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
    char buf[512];
    struct pollfd pfd;
    pfd.fd      = sock;
    pfd.events  = POLLIN;
    pfd.revents = 0;

    /* Read up to 512 bytes per syscall instead of one character at a time.
       A typical HTTP Range request is ~200 bytes — this reduces syscall
       overhead from ~200 to 1 per request, significantly lowering first-byte
       latency of the audio stream response. */
    while (raw.size() < 8192) {
        pfd.revents = 0;
        int ready = vozduxan_poll(&pfd, 1, HTTP_RECV_TIMEOUT_MS);
        if (ready <= 0) return {};
        ssize_t n = recv(sock, buf, sizeof(buf), 0);
        if (n <= 0) return {};
        raw.append(buf, (size_t)n);
        if (raw.find("\r\n\r\n") != std::string::npos) break;
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

VozduxanSessionImpl::VozduxanSessionImpl(const VozduxanConfig& cfg)
    /* Copy storage_path into an owned std::string FIRST so we don't hold
       a raw pointer into Rust memory that will be dropped after this call
       returns.  cfg_.storage_path is never read after construction. */
    : storage_path_(cfg.storage_path ? cfg.storage_path : ""),
      cfg_(cfg),
      session_(lt::settings_pack{})
{
    cfg_.storage_path = nullptr; /* intentionally nulled — use storage_path_ */

    // Store log callback before the first VOZDUXAN_LOG call below.
    log_fn_       = cfg.log_fn;
    log_userdata_ = cfg.log_userdata;

    winsock_init();

    if (cfg_.cache_max_bytes == 0) cfg_.cache_max_bytes = 50ULL * 1024 * 1024 * 1024;
    if (cfg_.cache_ttl_secs  == 0) cfg_.cache_ttl_secs  = 3600;

    fs::create_directories(storage_path_);
    dht_state_path_ = (fs::path(storage_path_) / "dht_state.dat").string();

    init_session();
    load_dht_state();

    alert_thread_ = std::thread([this]{ alert_loop(); });
    http_thread_  = std::thread([this]{ http_server_loop(); });

    /* give server a moment to bind */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    VOZDUXAN_LOG("session created — storage=%s http_port=%d",
             storage_path_.c_str(), (int)http_port_);
}

VozduxanSessionImpl::~VozduxanSessionImpl() {
    save_dht_state();

    /* 1. Signal every loop to exit and every serve_range to abort.
          Do this BEFORE joining any threads so they see the flags on their
          next iteration and don't block for a full timeout interval. */
    running_.store(false);
    {
        std::lock_guard<std::mutex> lock(streams_mutex_);
        for (auto& [token, st] : streams_) {
            st->stop_flag.store(true);
            st->abort_http.store(true);
            st->seek_generation.fetch_add(1, std::memory_order_release);
        }
    }

    session_.abort();

    /* 2. Close the server socket — unblocks the accept() poll in http_server_loop. */
    if (server_fd_ != kInvalidSock) {
        shutdown_sock(server_fd_);
        close_sock(server_fd_);
        server_fd_ = kInvalidSock;
    }

    /* 3. Join background threads in order:
          alert_loop exits when running_=false (polls every 200 ms).
          http_server_loop exits when running_=false (polls every 500 ms). */
    if (alert_thread_.joinable()) alert_thread_.join();
    if (http_thread_.joinable())  http_thread_.join();

    /* 4. Join per-connection threads.
          They check running_ in their keep-alive loop and abort_http in
          serve_range, so they exit within HTTP_RECV_TIMEOUT_MS (5 s) at most.
          Most entries will already have done=true (pruned during normal
          operation); we join the remaining ones that are still in-flight. */
    {
        std::lock_guard<std::mutex> lock(conn_threads_mutex_);
        for (auto& entry : conn_threads_) {
            if (entry->th.joinable()) entry->th.join();
        }
    }

    /* 5. Join priority workers (serve_range has already stopped, safe to join). */
    {
        std::lock_guard<std::mutex> lock(streams_mutex_);
        for (auto& [token, st] : streams_) {
            if (st->priority_thread.joinable()) st->priority_thread.join();
        }
    }

    winsock_cleanup();
}

/* ════════════════════════════════════════════════════════════════════════
 *  libtorrent session init
 * ════════════════════════════════════════════════════════════════════════ */

void VozduxanSessionImpl::init_session() {
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
    /* Default libtorrent is ~15s; values like 4s abort many handshakes before ut_metadata. */
    sp.set_int(lt::settings_pack::peer_connect_timeout,   15);
    sp.set_int(lt::settings_pack::num_want,              200);

    sp.set_bool(lt::settings_pack::announce_to_all_tiers,    true);
    sp.set_bool(lt::settings_pack::announce_to_all_trackers, true);

    /* Upload slots — Give-to-Get: maintain unchoke reciprocity */
    sp.set_int(lt::settings_pack::unchoke_slots_limit, 8);

    /* Read-ahead line size: 32 blocks (name differs when TORRENT_ABI_VERSION > 2). */
#if TORRENT_ABI_VERSION <= 2
    sp.set_int(lt::settings_pack::read_cache_line_size, 32);
#else
    sp.set_int(lt::settings_pack::deprecated_read_cache_line_size, 32);
#endif

    /* DHT + LSD for peer discovery */
    sp.set_bool(lt::settings_pack::enable_dht,   true);
    sp.set_bool(lt::settings_pack::enable_lsd,   true);
    sp.set_bool(lt::settings_pack::enable_upnp,  true);

    sp.set_int(lt::settings_pack::alert_mask,
               lt::alert_category::status |
               lt::alert_category::piece_progress |
               lt::alert_category::storage |  /* read_piece_alert lives here */
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
 *  DHT state persistence
 * ════════════════════════════════════════════════════════════════════════ */

void VozduxanSessionImpl::load_dht_state() {
    std::ifstream f(dht_state_path_, std::ios::binary);
    if (!f) return;
    std::string buf((std::istreambuf_iterator<char>(f)), {});
    lt::error_code ec;
    lt::bdecode_node const n = lt::bdecode(
        lt::span<char const>(buf.data(), static_cast<int>(buf.size())), ec);
    if (ec) {
        VOZDUXAN_LOG("load_dht_state: bdecode error: %s", ec.message().c_str());
        return;
    }
    lt::session_params sp = lt::read_session_params(n, lt::session::save_dht_state);
    session_.set_dht_state(std::move(sp.dht_state));
    VOZDUXAN_LOG("load_dht_state: loaded %zu bytes from %s",
                 buf.size(), dht_state_path_.c_str());
}

void VozduxanSessionImpl::save_dht_state() {
    if (dht_state_path_.empty()) return;
    lt::session_params const sp =
        session_.session_state(lt::session::save_dht_state);
    std::vector<char> const buf =
        lt::write_session_params_buf(sp, lt::session::save_dht_state);
    std::ofstream f(dht_state_path_, std::ios::binary | std::ios::trunc);
    if (!f) {
        VOZDUXAN_LOG("save_dht_state: cannot open %s for writing", dht_state_path_.c_str());
        return;
    }
    f.write(buf.data(), (std::streamsize)buf.size());
    VOZDUXAN_LOG("save_dht_state: saved %zu bytes to %s",
                 buf.size(), dht_state_path_.c_str());
}

/* ════════════════════════════════════════════════════════════════════════
 *  Alert processing thread
 * ════════════════════════════════════════════════════════════════════════ */

void VozduxanSessionImpl::alert_loop() {
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

void VozduxanSessionImpl::on_read_piece(lt::read_piece_alert* rpa) {
    if (rpa->error) {
        VOZDUXAN_LOG("read_piece ERROR piece=%d: %s",
                 (int)rpa->piece, rpa->error.message().c_str());
        /* Fulfill any waiters with empty data so serve_range fails fast rather
         * than blocking until PIECE_TIMEOUT_MS.  Previously this returned early
         * without fulfilling the promise, leaving wait_for_piece blocked for up
         * to 20 s while the browser timed out the HTTP connection at ~9 s.
         *
         * IMPORTANT: use continue (not break) when no waiter is found for a
         * stream.  Multiple streams can share the same torrent handle (e.g. a
         * current-playback stream and a prefetch/hover stream from the same
         * album).  If we break on the first handle-match that has no waiter, the
         * actual waiting stream is never unblocked and wait_for_piece blocks for
         * the full PIECE_TIMEOUT_MS while the browser times out at ~9 s. */
        std::lock_guard<std::mutex> slock(streams_mutex_);
        for (auto& [token, st] : streams_) {
            if (st->handle != rpa->handle) continue;
            std::lock_guard<std::mutex> wlock(st->waiters_mutex);
            auto it = st->waiters.find((int)rpa->piece);
            if (it == st->waiters.end()) continue; /* keep scanning siblings */
            for (auto& promise : it->second.promises) {
                try { promise.set_value({}); }
                catch (const std::future_error&) {}
            }
            st->waiters.erase(it);
            /* keep going — a sibling stream may also be waiting for this piece */
        }
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
        if (it == st->waiters.end()) continue; /* keep scanning siblings */

        for (auto& promise : it->second.promises) {
            try { promise.set_value(data); }
            catch (const std::future_error&) {}
        }
        st->waiters.erase(it);
        /* keep going — a sibling stream may also be waiting for this piece */
    }
}

void VozduxanSessionImpl::on_metadata_received(lt::metadata_received_alert* a) {
    VOZDUXAN_LOG("metadata_received alert — scanning streams for match");
    std::lock_guard<std::mutex> lock(streams_mutex_);
    int matched = 0;
    for (auto& [token, st] : streams_) {
        if (st->handle == a->handle) {
            st->metadata_ready.store(true, std::memory_order_release);
            st->metadata_cv.notify_all();
            VOZDUXAN_LOG("metadata_received -> token=%s notified", token.c_str());
            ++matched;
        }
    }
    if (matched == 0) {
        VOZDUXAN_LOG("metadata_received — no matching stream found (handle registered late?)");
    }
}

void VozduxanSessionImpl::on_metadata_failed(lt::metadata_failed_alert* a) {
    VOZDUXAN_LOG("metadata_failed alert");
    std::lock_guard<std::mutex> lock(streams_mutex_);
    for (auto& [token, st] : streams_) {
        if (st->handle == a->handle) {
            st->metadata_failed.store(true, std::memory_order_release);
            st->metadata_cv.notify_all();
            VOZDUXAN_LOG("metadata_failed -> token=%s notified", token.c_str());
        }
    }
}

/* ════════════════════════════════════════════════════════════════════════
 *  HTTP server
 * ════════════════════════════════════════════════════════════════════════ */

void VozduxanSessionImpl::http_server_loop() {
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
        if (vozduxan_poll(&pfd, 1, 500) <= 0) continue;

        sock_t client = accept(server_fd_, nullptr, nullptr);
        if (client == kInvalidSock) continue;

        /* Spawn a new per-connection thread.  Use a shared ConnEntry so we
           can store a raw pointer in the lambda (the shared_ptr in the
           vector keeps the object alive) and set done=true when the thread
           exits.  Before pushing, prune all finished entries so the vector
           stays bounded to the number of currently-active connections.    */
        auto entry = std::make_shared<ConnEntry>();
        entry->th = std::thread([this, client, e = entry.get()]() {
            handle_http_connection(client);
            close_sock(client);
            e->done.store(true, std::memory_order_release);
        });

        {
            std::lock_guard<std::mutex> lock(conn_threads_mutex_);
            for (auto it = conn_threads_.begin(); it != conn_threads_.end(); ) {
                if ((*it)->done.load(std::memory_order_acquire)) {
                    (*it)->th.join(); /* instant — thread already exited */
                    it = conn_threads_.erase(it);
                } else {
                    ++it;
                }
            }
            conn_threads_.push_back(std::move(entry));
        }
    }
}

void VozduxanSessionImpl::handle_http_connection(sock_t sock) {
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

        /* Snapshot generation for this response. Do NOT bump on Range: media
         * players often open parallel byte-range requests (e.g. start + tail for
         * tags); bumping would cancel sibling transfers and truncate the body. */
        uint64_t gen = st->seek_generation.load(std::memory_order_acquire);

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

bool VozduxanSessionImpl::serve_range(sock_t sock, StreamState& stream,
                                  int64_t start, int64_t end,
                                  uint64_t gen) {
    int piece_len = stream.ti->piece_length();
    int64_t pos   = start;

    while (pos <= end && running_.load()) {
        if (stream.abort_http.load()) return false;

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
 *  Direct piece read (synchronous file I/O)
 * ════════════════════════════════════════════════════════════════════════ */

std::vector<char> VozduxanSessionImpl::read_piece_direct(
        int piece_idx, const StreamState& stream) {

    auto& ti        = *stream.ti;
    auto& files     = ti.files();
    int   piece_len = ti.piece_length();
    int   num_pieces= ti.num_pieces();

    int64_t logical_start = (int64_t)piece_idx * piece_len;
    int64_t piece_size    = (piece_idx == num_pieces - 1)
                            ? ti.total_size() - logical_start
                            : piece_len;

    /* Zero-initialised: slices from adjacent files are left as zeros.
     *
     * Why we only read the target audio file's slices:
     *
     *   Boundary pieces (first_piece, last_piece) span MULTIPLE files.
     *   Adjacent files are typically set to dont_download; libtorrent may
     *   mark the piece "have" once the audio portion is verified, yet never
     *   create the adjacent files on disk (their directories may not even
     *   exist).  Attempting to open those files would always fail and, in the
     *   old code, forced a 60-second fallback to async read_piece().
     *
     *   serve_range reads piece_data starting at (file_offset % piece_len),
     *   which is inside the audio file's slice.  Zeros in the pre-audio-file
     *   or post-audio-file regions of the piece data are never transmitted to
     *   the browser.
     *
     *   If the audio file's own slice cannot be opened that is a genuine I/O
     *   error — return {} so wait_for_piece falls back to async read_piece.   */
    std::vector<char> data((size_t)piece_size, '\0');

    auto slices = files.map_block(lt::piece_index_t(piece_idx), 0, (int)piece_size);
    int64_t buf_off = 0;

    for (auto const& sl : slices) {
        if (sl.size == 0) { buf_off += sl.size; continue; }

        if (sl.file_index != lt::file_index_t(stream.file_idx)) {
            /* Adjacent file — skip without even attempting to open it.
               The zeros already in data[] at this offset are intentional. */
            buf_off += sl.size;
            continue;
        }

        /* Audio file slice — read it. */
        std::string fpath = files.file_path(sl.file_index, storage_path_);
        std::ifstream f(fpath, std::ios::binary);
        if (!f) {
            VOZDUXAN_LOG("read_piece_direct(%d): cannot open audio file %s",
                     piece_idx, fpath.c_str());
            return {};
        }
        f.seekg((std::streamoff)sl.offset);
        f.read(data.data() + buf_off, (std::streamsize)sl.size);
        if (f.gcount() != (std::streamsize)sl.size) {
            VOZDUXAN_LOG("read_piece_direct(%d): short read in %s (got %lld / %lld)",
                     piece_idx, fpath.c_str(),
                     (long long)f.gcount(), (long long)sl.size);
            return {};
        }
        buf_off += sl.size;
    }

    return data;
}

/* ════════════════════════════════════════════════════════════════════════
 *  Piece read synchronisation
 * ════════════════════════════════════════════════════════════════════════ */

std::vector<char> VozduxanSessionImpl::wait_for_piece(StreamState& stream,
                                                   int piece_idx,
                                                   uint64_t seek_gen) {
    /* Check if torrent is paused — call resume() so libtorrent can start
     * downloading.  Something (e.g. prioritize_files() async processing or a
     * libtorrent "finished" state transition) may have paused the torrent after
     * prepare() called handle.resume(); we repeat it here as a safety net. */
    if (stream.handle.flags() & lt::torrent_flags::paused) {
        VOZDUXAN_LOG("wait_for_piece(%d) WARNING: torrent is PAUSED — calling resume()",
                 piece_idx);
        stream.handle.resume();
    }

    /* If we already have this piece, read it directly from disk.
     *
     * Problem with the async read_piece() path:
     *   libtorrent dispatches read_piece() to the same disk I/O thread that handles
     *   writes.  When fast-start primes ~20 pieces with set_piece_deadline(0) and
     *   those pieces happen to arrive simultaneously (e.g. the target track starts on
     *   a piece that is the last piece of the previous track — already on disk — while
     *   pieces 1-N of the new track are arriving rapidly from peers), the disk I/O
     *   thread accumulates 10-20 pending writes ahead of our single read.  On macOS
     *   this stalls the first HTTP response body byte for 9+ seconds, which is exactly
     *   Safari / WebKit's media-source timeout → MEDIA_ERR_SRC_NOT_SUPPORTED.
     *
     *   Raising PIECE_TIMEOUT_MS from 2 s to 20 s only masked the symptom on the
     *   vozduxan side; the browser still gives up at ~9 s.
     *
     * Fix: read the piece synchronously via std::ifstream using file_storage::map_block
     *   to locate the data.  This completely bypasses the disk I/O thread and finishes
     *   in < 5 ms on SSD.  Async read_piece() is kept as a fallback in case the direct
     *   read fails (e.g. the torrent uses a non-default storage backend). */
    if (stream.handle.have_piece(lt::piece_index_t(piece_idx))) {
        if (stream.direct_read_ok.load(std::memory_order_relaxed)) {
            auto data = read_piece_direct(piece_idx, stream);
            if (!data.empty()) return data;
            VOZDUXAN_LOG("wait_for_piece(%d) direct read failed — "
                         "disabling for this stream, falling back to async read_piece",
                         piece_idx);
            stream.direct_read_ok.store(false, std::memory_order_relaxed);
        }

        /* Async fallback: unchanged from original, but now also checks abort/seek
         * so that releasing the stream unblocks the serving thread immediately. */
        std::future<std::vector<char>> future;
        {
            std::lock_guard<std::mutex> lock(stream.waiters_mutex);
            std::promise<std::vector<char>> promise;
            future = promise.get_future();
            stream.waiters[piece_idx].promises.push_back(std::move(promise));
        }
        stream.handle.read_piece(lt::piece_index_t(piece_idx));

        auto t0       = std::chrono::steady_clock::now();
        auto deadline = t0 + std::chrono::milliseconds(PIECE_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            if (stream.abort_http.load()) {
                std::lock_guard<std::mutex> lock(stream.waiters_mutex);
                stream.waiters.erase(piece_idx);
                return {};
            }
            if (stream.seek_generation.load(std::memory_order_acquire) != seek_gen) {
                std::lock_guard<std::mutex> lock(stream.waiters_mutex);
                stream.waiters.erase(piece_idx);
                return {};
            }
            auto status = future.wait_for(std::chrono::milliseconds(50));
            if (status == std::future_status::ready) {
                try { return future.get(); } catch (...) { return {}; }
            }
        }
        auto waited_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        VOZDUXAN_LOG("wait_for_piece(%d) read_piece TIMEOUT after %lldms — disk I/O thread busy?",
                 piece_idx, (long long)waited_ms);
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

    /* Ensure the piece is not stuck at dont_download (priority 0) before
     * calling set_piece_deadline — set_piece_deadline is a silent no-op on
     * pieces with dont_download priority, which can happen when the async
     * prioritize_files() call hasn't completed yet or the priority worker
     * hasn't reached this piece yet. */
    stream.handle.piece_priority(lt::piece_index_t(piece_idx), lt::top_priority);
    stream.handle.set_piece_deadline(
        lt::piece_index_t(piece_idx),
        0,
        lt::torrent_handle::alert_when_available
    );

    /* Poll with short intervals so we can detect seek cancellation */
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(PIECE_TIMEOUT_MS);

    int resume_tick = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        /* abort if a newer seek arrived */
        if (stream.seek_generation.load(std::memory_order_acquire) != seek_gen) {
            std::lock_guard<std::mutex> lock(stream.waiters_mutex);
            stream.waiters.erase(piece_idx);
            return {};
        }

        auto status = future.wait_for(std::chrono::milliseconds(100));
        if (status == std::future_status::ready) {
            std::lock_guard<std::mutex> lock(stream.waiters_mutex);
            stream.waiters.erase(piece_idx);
            try { return future.get(); } catch (...) { return {}; }
        }

        /* Poll path: the priority worker calls set_piece_deadline() every
         * PRIORITY_TICK_MS (100 ms) WITHOUT alert_when_available, which
         * overwrites and clears the flag we set above.  As a result
         * read_piece_alert may never fire, leaving the future unresolved
         * for the full PIECE_TIMEOUT_MS (60 s) even though the piece is
         * already on disk.  Detect arrival here directly so we never wait
         * longer than one 100 ms tick after the piece lands. */
        if (stream.handle.have_piece(lt::piece_index_t(piece_idx)) &&
                stream.direct_read_ok.load(std::memory_order_relaxed)) {
            auto data = read_piece_direct(piece_idx, stream);
            if (!data.empty()) {
                std::lock_guard<std::mutex> lock(stream.waiters_mutex);
                stream.waiters.erase(piece_idx);
                return data;
            }
            /* read_piece_direct failed despite have_piece — disable for this
             * stream; the future path (async read_piece) will resolve it. */
            stream.direct_read_ok.store(false, std::memory_order_relaxed);
        }

        /* Every ~1 s (10 × 100 ms): re-assert deadline + alert_when_available
         * unconditionally (not only when paused) to counteract the priority
         * worker's flag-clearing calls.  Also call resume() if the torrent
         * somehow got paused again. */
        if (++resume_tick % 10 == 0) {
            if (stream.handle.flags() & lt::torrent_flags::paused) {
                VOZDUXAN_LOG("wait_for_piece(%d): torrent still paused after %ds — re-calling resume()",
                         piece_idx, resume_tick / 10);
                stream.handle.resume();
            }
            /* Re-assert priority in case prioritize_files() raced and reset it */
            stream.handle.piece_priority(lt::piece_index_t(piece_idx), lt::top_priority);
            stream.handle.set_piece_deadline(
                lt::piece_index_t(piece_idx), 0,
                lt::torrent_handle::alert_when_available);
            /* Heartbeat log every ~10 s so the debug console shows the engine
             * is alive during long waits (otherwise the log goes silent for up
             * to PIECE_TIMEOUT_MS and the app looks frozen).              */
            if (resume_tick % 100 == 0) {
                int elapsed_s = resume_tick / 10;
                auto prio = stream.handle.piece_priority(lt::piece_index_t(piece_idx));
                try {
                    auto s = stream.handle.status(
                        lt::torrent_handle::query_accurate_download_counters);
                    VOZDUXAN_LOG("wait_for_piece(%d) heartbeat: waited %ds, "
                             "rate=%d B/s peers=%d piece_prio=%d",
                             piece_idx, elapsed_s,
                             (int)s.download_rate, (int)s.num_peers,
                             (int)(uint8_t)prio);
                } catch (...) {
                    VOZDUXAN_LOG("wait_for_piece(%d) heartbeat: waited %ds piece_prio=%d",
                             piece_idx, elapsed_s, (int)(uint8_t)prio);
                }
            }
        }
    }

    VOZDUXAN_LOG("wait_for_piece(%d) TIMEOUT after %dms — paused=%s — trying 5s grace period",
             piece_idx, PIECE_TIMEOUT_MS,
             (stream.handle.flags() & lt::torrent_flags::paused) ? "YES" : "no");

    /* Grace-period: keep polling for up to 5 extra seconds.  The piece may
     * arrive just after the loop deadline (observed: skipped by <2 s in slow
     * swarms).  Without this, serve_range drops the connection and the browser
     * fires MEDIA_ERR_SRC_NOT_SUPPORTED, forcing a 1.5 s retry delay. */
    for (int grace = 0; grace < 50; ++grace) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (stream.abort_http.load()) break;
        if (stream.seek_generation.load(std::memory_order_acquire) != seek_gen) break;
        if (stream.handle.have_piece(lt::piece_index_t(piece_idx)) &&
                stream.direct_read_ok.load(std::memory_order_relaxed)) {
            auto data = read_piece_direct(piece_idx, stream);
            if (!data.empty()) {
                VOZDUXAN_LOG("wait_for_piece(%d) grace-period direct read succeeded after +%dms",
                         piece_idx, (grace + 1) * 100);
                std::lock_guard<std::mutex> lock(stream.waiters_mutex);
                stream.waiters.erase(piece_idx);
                return data;
            }
            stream.direct_read_ok.store(false, std::memory_order_relaxed);
        }
    }

    VOZDUXAN_LOG("wait_for_piece(%d) giving up after grace period", piece_idx);
    std::lock_guard<std::mutex> lock(stream.waiters_mutex);
    stream.waiters.erase(piece_idx);
    return {};
}

/* ════════════════════════════════════════════════════════════════════════
 *  Three-tier piece priority worker
 *  Adapted from Tribler streaming research (TU Delft, MIT licence)
 * ════════════════════════════════════════════════════════════════════════ */

void VozduxanSessionImpl::run_priority_worker(std::shared_ptr<StreamState> stream) {
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

        /* ── Set priority on all file pieces ────────────────────────────
         * Only call piece_priority() when the tier assignment actually changes.
         * Calling it for every piece every tick saturates libtorrent's internal
         * message queue on large files (e.g. a 300 MB FLAC with 1200 pieces
         * would queue 1200 messages per 100 ms tick even when nothing changed).
         * ─────────────────────────────────────────────────────────────────── */
        int total_pieces = ti->num_pieces();
        auto prios = stream->handle.get_piece_priorities();
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

            /* Only send the message when the priority actually changes. */
            if ((size_t)i < prios.size() && prios[(size_t)i] == prio) continue;
            stream->handle.piece_priority(lt::piece_index_t(i), prio);
        }

        /* ── Look-ahead past last_piece ──────────────────────────────────
         * Pre-warm the first few pieces of the NEXT track (same torrent).
         * These are the boundary+1 pieces that start immediately after this
         * file's last_piece.  By keeping them at default_priority we signal
         * "interested" to peers who have those pieces and stay unchoked for
         * the next prepare() call.  Without this, peers drop to "not
         * interested" state when we switch files and need a full unchoke
         * round (~10 s) before they start uploading again — causing the
         * observed 40-second stalls on large FLAC pieces.               */
        constexpr int NEXT_TRACK_LOOKAHEAD = 6; /* ~12 MB of look-ahead at 2 MB/piece */
        for (int i = stream->last_piece + 1;
             i < std::min(stream->last_piece + 1 + NEXT_TRACK_LOOKAHEAD, total_pieces);
             ++i) {
            /* Only touch pieces that are currently dont_download — avoid
               downgrading pieces that a sibling stream (hover/prefetch) has
               already elevated to a higher priority.                      */
            if ((size_t)i < prios.size() &&
                prios[(size_t)i] == lt::dont_download) {
                stream->handle.piece_priority(lt::piece_index_t(i), lt::default_priority);
            }
        }
    }
}

/* ════════════════════════════════════════════════════════════════════════
 *  add_torrent_params helper
 * ════════════════════════════════════════════════════════════════════════ */

lt::add_torrent_params VozduxanSessionImpl::make_atp(const char*    magnet,
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
        std::string    mnorm = normalize_magnet_btih_hex(std::string(magnet));
        lt::error_code ec;
        atp = lt::parse_magnet_uri(mnorm.c_str(), ec);
        if (ec) throw std::runtime_error(ec.message());
    } else {
        throw std::runtime_error("no magnet or torrent data");
    }

    atp.save_path = storage_path_;
    atp.flags |= lt::torrent_flags::sequential_download;
    atp.flags &= ~lt::torrent_flags::auto_managed;
    atp.flags &= ~lt::torrent_flags::paused;

    /* Append open trackers so thin-seeded torrents can find peers. */
    for (const auto& url : OPEN_TRACKERS)
        atp.trackers.push_back(url);

    /* Cap connections per torrent to avoid hammering low-seeder swarms. */
    atp.max_connections = 100;

    return atp;
}

/* ════════════════════════════════════════════════════════════════════════
 *  prepare()
 * ════════════════════════════════════════════════════════════════════════ */

VozduxanStreamInfo VozduxanSessionImpl::prepare(const char*    magnet,
                                         const uint8_t* torrent_data,
                                         size_t         torrent_len,
                                         int            file_idx,
                                         int            is_main,
                                         VozduxanProgressFn progress_fn,
                                         void*          userdata) {
    VozduxanStreamInfo info{};

    auto report = [&](float p, const char* s) {
        if (progress_fn) progress_fn(p, s, userdata);
    };

    VOZDUXAN_LOG("prepare() start — file_idx=%d has_torrent_data=%s",
             file_idx, (torrent_data && torrent_len > 0) ? "yes" : "no");

    try {
        lt::add_torrent_params atp = make_atp(magnet, torrent_data, torrent_len);
        VOZDUXAN_LOG("prepare() atp built");

        /* Grab a generation stamp.
         *
         * Only MAIN prepares (is_main != 0) increment prepare_gen_.  This
         * means a new main-track request cancels any in-flight fast-start —
         * whether it belongs to an old main prepare or a background prefetch.
         *
         * Hover-prefetch (is_main == 0) does NOT increment the counter: it
         * must never cancel the active playback stream's loading sequence.
         *
         * All callers read prepare_gen_ in their fast-start loop and abort
         * when they detect a newer main prepare has arrived.
         */
        if (is_main) {
            prepare_gen_.fetch_add(1, std::memory_order_acq_rel);
        }
        uint64_t my_gen = prepare_gen_.load(std::memory_order_acquire);

        std::string token = generate_token();
        auto st = std::make_shared<StreamState>();
        st->file_idx = file_idx;

        report(0.05f, "Connecting to swarm...");
        VOZDUXAN_LOG("prepare(%s) add_torrent...", token.c_str());
        lt::torrent_handle handle = session_.add_torrent(atp);
        st->handle = handle;
        VOZDUXAN_LOG("prepare(%s) add_torrent done — valid=%s paused=%s",
                 token.c_str(),
                 handle.is_valid() ? "yes" : "no",
                 (handle.flags() & lt::torrent_flags::paused) ? "YES (WARNING)" : "no");
        handle.force_reannounce(0); /* announce immediately; don't wait for tracker interval */

        /* If this is a main prepare, stop the priority workers of any SIBLING
           streams that share the same torrent handle (i.e. other tracks from the
           same album torrent that are still in-flight or haven't been released yet).
           Those workers keep calling set_piece_deadline() for the OLD file's pieces,
           which competes directly with our new first piece and can starve it for the
           entire fast-start window (8 s) plus wait_for_piece timeout (20 s).
           We only set stop_flag here; the actual thread join happens later in
           release_stream() when the caller drops the old token. */
        if (is_main) {
            std::lock_guard<std::mutex> slock(streams_mutex_);
            for (auto& [tok, other] : streams_) {
                if (other->handle == handle) {
                    bool was_stopped = other->stop_flag.exchange(true);
                    if (!was_stopped) {
                        VOZDUXAN_LOG("prepare(%s) stopping sibling priority worker on token=%s "
                                     "(same torrent, old file — clearing competing deadlines)",
                                     token.c_str(), tok.c_str());
                    }
                }
            }
        }

        /* Check for immediately-available metadata (torrent data provided,
           not a magnet-only add).  libtorrent does NOT fire
           metadata_received_alert when atp.ti is already set. */
        {
            auto ti_immed = handle.torrent_file();
            if (ti_immed) {
                st->ti = ti_immed;
                st->metadata_ready.store(true, std::memory_order_release);
                VOZDUXAN_LOG("prepare(%s) metadata immediate (torrent data path)", token.c_str());
            }
        }

        /* Register the stream (handle already set above). */
        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_[token] = st;
        }

        /* ── Wait for metadata using condition variable (magnet-only path) */
        if (!st->metadata_ready.load(std::memory_order_acquire)) {
            VOZDUXAN_LOG("prepare(%s) waiting for metadata (magnet path, timeout=%ds)...",
                     token.c_str(), METADATA_TIMEOUT_S);
            report(0.08f, "Resolving metadata...");

            /* Poll in 200 ms slices so we can detect prepare_gen_ changes and
               abort when a newer track is requested. */
            auto meta_deadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(METADATA_TIMEOUT_S);
            while (std::chrono::steady_clock::now() < meta_deadline) {
                {
                    std::unique_lock<std::mutex> lk(st->metadata_mtx);
                    st->metadata_cv.wait_for(lk, std::chrono::milliseconds(200),
                        [&]{ return st->metadata_ready.load() || st->metadata_failed.load(); });
                }
                if (st->metadata_ready.load() || st->metadata_failed.load()) break;

                if (prepare_gen_.load(std::memory_order_acquire) != my_gen) {
                    VOZDUXAN_LOG("prepare(%s) CANCELLED during metadata wait — newer prepare started",
                             token.c_str());
                    st->stop_flag.store(true);
                    st->abort_http.store(true);
                    bool has_sibling = false;
                    {
                        std::lock_guard<std::mutex> lock(streams_mutex_);
                        streams_.erase(token);
                        for (auto& [tok, other] : streams_) {
                            if (other->handle == handle) { has_sibling = true; break; }
                        }
                    }
                    if (!has_sibling) {
                        handle.pause();
                        std::lock_guard<std::mutex> ilock(idle_mutex_);
                        idle_torrents_.push_back({handle, std::chrono::steady_clock::now()});
                    } else {
                        VOZDUXAN_LOG("prepare(%s) cancel: torrent NOT paused — sibling stream active",
                                 token.c_str());
                    }
                    info.error = VOZDUXAN_ERR_INTERNAL;
                    snprintf(info.error_msg, sizeof(info.error_msg),
                             "Cancelled: superseded by newer prepare");
                    return info;
                }
            }

            /* Last-chance fallback in case alert fired before stream was registered */
            if (!st->metadata_ready.load()) {
                auto ti = handle.torrent_file();
                if (ti) { st->ti = ti; st->metadata_ready.store(true); }
            }

            if (st->metadata_failed.load())
                VOZDUXAN_LOG("prepare(%s) metadata FAILED", token.c_str());
            else if (st->metadata_ready.load())
                VOZDUXAN_LOG("prepare(%s) metadata received via alert", token.c_str());
            else
                VOZDUXAN_LOG("prepare(%s) metadata TIMED OUT after %ds", token.c_str(), METADATA_TIMEOUT_S);
        }

        if (!st->metadata_ready.load()) {
            VOZDUXAN_LOG("prepare(%s) FAIL: metadata not ready, removing torrent", token.c_str());
            session_.remove_torrent(handle);
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
            info.error = VOZDUXAN_ERR_METADATA_TIMEOUT;
            snprintf(info.error_msg, sizeof(info.error_msg),
                     "Metadata timeout after %d s", METADATA_TIMEOUT_S);
            return info;
        }

        if (!st->ti) {
            st->ti = handle.torrent_file();
        }

        if (!st->ti) {
            VOZDUXAN_LOG("prepare(%s) FAIL: torrent_file() null after metadata ready", token.c_str());
            session_.remove_torrent(handle);
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
            info.error = VOZDUXAN_ERR_METADATA_TIMEOUT;
            snprintf(info.error_msg, sizeof(info.error_msg), "torrent_file() null after metadata");
            return info;
        }

        /* ── Resolve file ─────────────────────────────────────────────── */
        auto& files     = st->ti->files();
        int   num_files = files.num_files();

        VOZDUXAN_LOG("prepare(%s) torrent has %d files, requesting file_idx=%d",
                 token.c_str(), num_files, file_idx);

        if (file_idx < 0 || file_idx >= num_files) {
            VOZDUXAN_LOG("prepare(%s) FAIL: file_idx %d out of range", token.c_str(), file_idx);
            session_.remove_torrent(handle);
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
            info.error = VOZDUXAN_ERR_INVALID_FILE;
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

        VOZDUXAN_LOG("prepare(%s) file=%s size=%lld mime=%s pieces=[%d..%d] piece_len=%d",
                 token.c_str(),
                 files.file_name(lt::file_index_t(file_idx)).to_string().c_str(),
                 (long long)st->file_size,
                 st->mime_type.c_str(),
                 st->first_piece, st->last_piece, piece_len);

        /* ── Pin file priorities ──────────────────────────────────────── */
        if (is_main) {
            /* Main prepare: concentrate bandwidth on the target file.
             *
             * Adjacent files (±1) are kept at low_priority instead of
             * dont_download.  This preserves the "interested" signal to peers
             * that happen to share pieces with the boundary region.  Without
             * this, a peer that only holds pieces 0..N-1 (the just-played
             * track's range) receives "not interested" the moment we switch
             * tracks, causing it to choke us.  The subsequent unchoke round
             * runs on a ~10 s timer, producing the observed 10–20 s stall
             * while we wait for new peers that have the first piece of the
             * next track.  Keeping adjacent files at low_priority means those
             * peers stay unchoked; if they also have the new first piece they
             * will serve it at top priority immediately. */
            std::vector<lt::download_priority_t> file_prios(
                (size_t)num_files, lt::dont_download);
            file_prios[(size_t)file_idx] = lt::top_priority;
            if (file_idx > 0)
                file_prios[(size_t)(file_idx - 1)] = lt::low_priority;
            if (file_idx + 1 < num_files)
                file_prios[(size_t)(file_idx + 1)] = lt::low_priority;
            handle.prioritize_files(file_prios);
        } else {
            /* Hover / prefetch: boost only the target file's pieces with
               per-piece calls — never touch the rest of the torrent.
               prioritize_files() resets every piece globally and would set
               any concurrent main prepare's pieces to dont_download, signalling
               "not interested" to peers and cancelling their in-flight requests
               (observed as 8–20 s stalls when a hover fires mid fast-start). */
            for (int i = st->first_piece; i <= st->last_piece; ++i) {
                handle.piece_priority(lt::piece_index_t(i), lt::top_priority);
            }
        }

        /* Ensure the torrent is running. Normally it stays active between
         * prepares (peer connections are preserved in release_stream), so
         * this is typically a no-op. */
        if (handle.flags() & lt::torrent_flags::paused) {
            VOZDUXAN_LOG("prepare(%s) torrent was paused — resuming (unexpected in normal flow)", token.c_str());
        }
        handle.resume();

        /* ── Fast-start ───────────────────────────────────────────────────
         *
         * Main prepare (is_main=1):
         *   Prime first HIGH_PRIORITY_PIECES with set_piece_deadline and wait
         *   up to FAST_START_TIMEOUT_MS for piece 0 — full aggressive prebuffer.
         *   Aborts early if prepare_gen_ changes (newer track requested).
         *
         * Hover / prefetch (is_main=0):
         *   Prime only the very first piece and wait at most
         *   HOVER_FAST_START_TIMEOUT_MS (500 ms).  Priming 20 pieces and
         *   waiting 8 s would compete with the active playback download and
         *   starve the main prepare that is likely running concurrently.
         * ─────────────────────────────────────────────────────────────── */
        report(0.60f, "Buffering...");

        /* Clear any stale piece deadlines before setting fresh ones.
           Sibling priority workers (stopped above) may have left deadlines for
           the old file's pieces on this handle.  Without clearing them, those
           lingering deadlines compete with our new first piece even after the
           workers exit, because libtorrent honours deadlines until explicitly
           cleared or the piece is downloaded. */
        handle.clear_piece_deadlines();

        if (is_main) {
            /* Full aggressive prebuffer for the track the user just picked. */
            int primed = 0;
            for (int i = st->first_piece;
                 i < std::min(st->first_piece + HIGH_PRIORITY_PIECES, st->last_piece + 1);
                 ++i) {
                /* Boundary pieces (first_piece, last_piece) may be shared with
                 * adjacent files.  prioritize_files() processes files sequentially;
                 * if the adjacent file's dont_download priority is applied after
                 * this file's top_priority, the shared piece ends up as
                 * dont_download.  set_piece_deadline() is a no-op on dont_download
                 * pieces, so we explicitly pin each piece to top_priority first. */
                handle.piece_priority(lt::piece_index_t(i), lt::top_priority);
                handle.set_piece_deadline(lt::piece_index_t(i), 0);
                ++primed;
            }
            VOZDUXAN_LOG("prepare(%s) fast-start: primed %d pieces, waiting for piece %d...",
                     token.c_str(), primed, st->first_piece);

            auto fast_start_begin = std::chrono::steady_clock::now();
            auto fast_start_dl    = fast_start_begin +
                                    std::chrono::milliseconds(FAST_START_TIMEOUT_MS);
            while (std::chrono::steady_clock::now() < fast_start_dl) {
                /* Abort if a newer main prepare() arrived. */
                if (prepare_gen_.load(std::memory_order_acquire) != my_gen) {
                    VOZDUXAN_LOG("prepare(%s) CANCELLED during fast-start — newer prepare started",
                             token.c_str());
                    st->stop_flag.store(true);
                    st->abort_http.store(true);
                    bool has_sibling = false;
                    {
                        std::lock_guard<std::mutex> lock(streams_mutex_);
                        streams_.erase(token);
                        for (auto& [tok, other] : streams_) {
                            if (other->handle == handle) { has_sibling = true; break; }
                        }
                    }
                    if (!has_sibling) {
                        handle.pause();
                        std::lock_guard<std::mutex> ilock(idle_mutex_);
                        idle_torrents_.push_back({handle, std::chrono::steady_clock::now()});
                    } else {
                        VOZDUXAN_LOG("prepare(%s) cancel: torrent NOT paused — sibling stream active",
                                 token.c_str());
                    }
                    info.error = VOZDUXAN_ERR_INTERNAL;
                    snprintf(info.error_msg, sizeof(info.error_msg),
                             "Cancelled: superseded by newer prepare");
                    return info;
                }

                if (handle.have_piece(lt::piece_index_t(st->first_piece))) break;

                /* Re-assert top priority and deadline on the first piece every
                 * poll tick.  prioritize_files() is dispatched to libtorrent's
                 * internal thread and may be processed after our initial
                 * piece_priority() + set_piece_deadline() calls.  In that case
                 * a boundary piece shared with an adjacent dont_download file
                 * could end up as dont_download, and set_piece_deadline is a
                 * no-op on dont_download pieces.  Repeating the assertion every
                 * 50 ms guarantees the request reaches libtorrent's piece-picker
                 * regardless of scheduling races or sequential_download-mode
                 * pointer state. */
                handle.piece_priority(lt::piece_index_t(st->first_piece),
                                      lt::top_priority);
                handle.set_piece_deadline(lt::piece_index_t(st->first_piece), 0);

                /* If the torrent was paused by libtorrent's internal logic after
                 * our prioritize_files() call (e.g. a "finished" state transition
                 * or an async auto-management race), re-assert resume() so that
                 * downloads can proceed.  The paused flag is re-checked every tick
                 * because resume() is asynchronous — one call may not be enough. */
                if (handle.flags() & lt::torrent_flags::paused) {
                    VOZDUXAN_LOG("prepare(%s) fast-start: torrent paused — re-calling resume()",
                             token.c_str());
                    handle.resume();
                }

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
            VOZDUXAN_LOG("prepare(%s) fast-start done in %lldms — piece %d ready=%s",
                     token.c_str(), (long long)elapsed_fast,
                     st->first_piece, piece0_ready ? "YES" : "NO (continuing anyway)");

        } else {
            /* Hover / prefetch: prime only piece 0 and wait at most 500 ms.
               Keeps libtorrent bandwidth available for the active main track. */
            handle.set_piece_deadline(lt::piece_index_t(st->first_piece), 0);
            VOZDUXAN_LOG("prepare(%s) hover fast-start: primed piece %d, waiting up to %dms...",
                     token.c_str(), st->first_piece, HOVER_FAST_START_TIMEOUT_MS);

            auto hover_dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(HOVER_FAST_START_TIMEOUT_MS);
            while (std::chrono::steady_clock::now() < hover_dl) {
                if (handle.have_piece(lt::piece_index_t(st->first_piece))) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }

            bool piece0_ready = handle.have_piece(lt::piece_index_t(st->first_piece));
            VOZDUXAN_LOG("prepare(%s) hover fast-start done — piece %d ready=%s",
                     token.c_str(), st->first_piece,
                     piece0_ready ? "YES" : "NO (continuing anyway)");
        }

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
        info.error     = VOZDUXAN_OK;

        VOZDUXAN_LOG("prepare(%s) SUCCESS — url=%s", token.c_str(), info.url);

    } catch (const std::exception& e) {
        VOZDUXAN_LOG("prepare() EXCEPTION: %s", e.what());
        info.error = VOZDUXAN_ERR_INTERNAL;
        snprintf(info.error_msg, sizeof(info.error_msg), "%s", e.what());
    }

    return info;
}

/* ════════════════════════════════════════════════════════════════════════
 *  list_files()
 * ════════════════════════════════════════════════════════════════════════ */

VozduxanFileList VozduxanSessionImpl::list_files(const char*    magnet,
                                          const uint8_t* torrent_data,
                                          size_t         torrent_len) {
    VozduxanFileList result{};

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

            /* Poll in 200 ms slices so the destructor's running_=false is
               detected quickly, avoiding a 90-second block during shutdown. */
            auto meta_deadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(METADATA_TIMEOUT_S);
            while (std::chrono::steady_clock::now() < meta_deadline) {
                if (!running_.load()) {
                    VOZDUXAN_LOG("list_files(%s) CANCELLED — session shutting down",
                             token.c_str());
                    std::lock_guard<std::mutex> lock(streams_mutex_);
                    streams_.erase(token);
                    result.error = VOZDUXAN_ERR_INTERNAL;
                    snprintf(result.error_msg, sizeof(result.error_msg),
                             "Cancelled: session shutting down");
                    return result;
                }
                {
                    std::unique_lock<std::mutex> lk(st->metadata_mtx);
                    st->metadata_cv.wait_for(lk, std::chrono::milliseconds(200),
                        [&]{ return st->metadata_ready.load() || st->metadata_failed.load(); });
                }
                if (st->metadata_ready.load() || st->metadata_failed.load()) break;
            }

            if (!st->metadata_ready.load()) {
                auto ti = handle.torrent_file();
                if (ti) { st->ti = ti; st->metadata_ready.store(true); }
            }

            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
        }

        if (!st->metadata_ready.load()) {
            {
                bool in_use = false;
                std::lock_guard<std::mutex> lock(streams_mutex_);
                for (auto& [tok, active] : streams_) {
                    if (active->handle == handle) { in_use = true; break; }
                }
                if (!in_use) session_.remove_torrent(handle);
            }
            result.error = VOZDUXAN_ERR_METADATA_TIMEOUT;
            snprintf(result.error_msg, sizeof(result.error_msg), "Metadata timeout");
            return result;
        }

        if (!st->ti) st->ti = handle.torrent_file();

        /* Only remove if no active stream is using this handle.
         * If the same magnet is currently being streamed, libtorrent returned the
         * existing handle (AlreadyManaged) and calling remove_torrent() here would
         * kill the active download. */
        {
            bool in_use = false;
            std::lock_guard<std::mutex> lock(streams_mutex_);
            for (auto& [tok, active] : streams_) {
                if (active->handle == handle) { in_use = true; break; }
            }
            if (!in_use) {
                session_.remove_torrent(handle);
                VOZDUXAN_LOG("list_files: torrent removed (was not in active streams)");
            } else {
                VOZDUXAN_LOG("list_files: torrent NOT removed — handle is used by an active stream");
            }
        }

        if (!st->ti) {
            result.error = VOZDUXAN_ERR_INTERNAL;
            snprintf(result.error_msg, sizeof(result.error_msg), "torrent_file() null");
            return result;
        }

        auto& files   = st->ti->files();
        int   n       = files.num_files();
        auto* entries = new VozduxanFileEntry[(size_t)n];

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
        result.error = VOZDUXAN_OK;

    } catch (const std::exception& e) {
        result.error = VOZDUXAN_ERR_INTERNAL;
        snprintf(result.error_msg, sizeof(result.error_msg), "%s", e.what());
    }

    return result;
}

/* ════════════════════════════════════════════════════════════════════════
 *  notify_position / release_stream / evict
 * ════════════════════════════════════════════════════════════════════════ */

void VozduxanSessionImpl::notify_position(const std::string& token,
                                       int64_t byte_offset) {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    auto it = streams_.find(token);
    if (it != streams_.end())
        it->second->playback_byte.store(byte_offset, std::memory_order_relaxed);
}

VozduxanSessionImpl::StreamStats
VozduxanSessionImpl::stream_stats(const std::string& token) {
    StreamStats result{0, 0};
    std::lock_guard<std::mutex> lock(streams_mutex_);
    auto it = streams_.find(token);
    if (it == streams_.end()) return result;
    try {
        auto s = it->second->handle.status(lt::torrent_handle::query_accurate_download_counters);
        result.download_rate_bytes = static_cast<int32_t>(s.download_rate);
        result.num_peers           = s.num_peers;
    } catch (...) {}
    return result;
}

void VozduxanSessionImpl::release_stream(const std::string& token) {
    VOZDUXAN_LOG("release_stream(%s) start", token.c_str());
    std::shared_ptr<StreamState> st;
    {
        std::lock_guard<std::mutex> lock(streams_mutex_);
        auto it = streams_.find(token);
        if (it == streams_.end()) {
            VOZDUXAN_LOG("release_stream(%s) token not found — already released?", token.c_str());
            return;
        }
        st = it->second;
        streams_.erase(it);
    }

    st->stop_flag.store(true);
    st->abort_http.store(true);
    /* bump generation so any blocking serve_range/wait_for_piece exits */
    st->seek_generation.fetch_add(1, std::memory_order_release);

    VOZDUXAN_LOG("release_stream(%s) joining priority_thread...", token.c_str());
    auto t0 = std::chrono::steady_clock::now();
    if (st->priority_thread.joinable())
        st->priority_thread.join();
    auto join_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    VOZDUXAN_LOG("release_stream(%s) priority_thread joined in %lldms", token.c_str(), (long long)join_ms);

    if (st->handle.is_valid()) {
        /* Only idle the torrent if no other active stream is still using the
         * same underlying torrent handle.  Two streams share a handle when the
         * second prepare() call gets an AlreadyManaged response from libtorrent
         * (same info hash, e.g. version-mismatch superseded stream, warm prefetch,
         * or hover-prefetch released while the current stream is still in flight).
         * Setting all priorities to dont_download on a shared handle would
         * kill the sibling stream's downloads. */
        bool shared = false;
        {
            std::lock_guard<std::mutex> slock(streams_mutex_);
            for (auto& [tok, other] : streams_) {
                if (other->handle == st->handle) {
                    shared = true;
                    break;
                }
            }
        }
        if (shared) {
            VOZDUXAN_LOG("release_stream(%s) torrent NOT paused — still used by another active stream", token.c_str());
        } else {
            /* Keep the torrent running with its current file priorities.
             *
             * Do NOT call prioritize_files(all: dont_download) here.
             * Setting every file to dont_download signals "not interested"
             * to all peers.  Peers respond by choking us, and the unchoke
             * round runs on a ~10 s timer — so the very next prepare() that
             * re-signals "interested" still waits up to 10 s before peers
             * restart sending pieces (observed as 8–20 s fast-start stalls).
             *
             * Leaving the existing file priorities intact (typically the last
             * played file at top_priority, others at dont_download) keeps us
             * "interested" in the swarm.  Peers stay unchoked.  The next
             * prepare() calls prioritize_files() for the new file, and pieces
             * arrive immediately from already-unchoked peers.
             *
             * Side-effect: the idle torrent briefly continues downloading the
             * last file's pieces.  The gap between release and the next
             * prepare() is typically < 200 ms, so the extra data is negligible.
             * TTL-based evict() handles long-term idle cleanup. */
            st->handle.clear_piece_deadlines();
            std::lock_guard<std::mutex> lock(idle_mutex_);
            idle_torrents_.push_back({st->handle,
                                      std::chrono::steady_clock::now()});
            VOZDUXAN_LOG("release_stream(%s) torrent idle — priorities unchanged, peers stay unchoked", token.c_str());
        }
    }
    VOZDUXAN_LOG("release_stream(%s) done", token.c_str());
}

void VozduxanSessionImpl::evict() {
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

} // namespace vozduxan
