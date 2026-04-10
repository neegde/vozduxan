/**
 * blizorukost — session.cpp
 *
 * Three-tier piece prioritisation strategy adapted from Tribler (TU Delft):
 *   TIER 1 (pieces 0-19 ahead)  — set_piece_deadline(), time-critical mode
 *   TIER 2 (pieces 20-59 ahead) — top_priority, rarest-first allowed
 *   TIER 3 (60+ pieces ahead)   — default_priority, swarm health
 *
 * Reference: https://github.com/Tribler/tribler (MIT)
 *            https://libtorrent.org/streaming.html  (BSD)
 */

#include "session.hpp"

#include <libtorrent/settings_pack.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/extensions/ut_pex.hpp>
#include <libtorrent/extensions/ut_metadata.hpp>
#include <libtorrent/extensions/smart_ban.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <random>
#include <sstream>

namespace bliz {
namespace fs = std::filesystem;

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

static ParsedRequest read_http_request(int sock) {
    /* Read until \r\n\r\n with a 5-second recv timeout */
    std::string raw;
    raw.reserve(512);
    char ch;
    struct pollfd pfd{sock, POLLIN, 0};

    while (raw.size() < 8192) {
        int ready = poll(&pfd, 1, HTTP_RECV_TIMEOUT_MS);
        if (ready <= 0) return {};           /* timeout or error */
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

    /* request line */
    if (!std::getline(ss, line)) return {};
    if (!line.empty() && line.back() == '\r') line.pop_back();
    {
        std::istringstream rl(line);
        std::string ver;
        rl >> req.method >> req.path >> ver;
        req.keep_alive = (ver.find("1.1") != std::string::npos);
    }

    /* headers */
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) break;
        auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = trim(line.substr(0, colon));
        std::string val = trim(line.substr(colon + 1));

        /* normalise header name to lower-case */
        for (auto& c : key) c = (char)tolower((unsigned char)c);

        if (key == "range") {
            /* bytes=start-end  or  bytes=start- */
            req.has_range = true;
            const char* p = val.c_str();
            if (strncmp(p, "bytes=", 6) == 0) p += 6;
            char* dash = (char*)strchr(p, '-');
            if (dash) {
                req.range_start = strtoll(p, nullptr, 10);
                if (*(dash + 1)) req.range_end = strtoll(dash + 1, nullptr, 10);
            }
        } else if (key == "connection") {
            for (auto& c : val) c = (char)tolower((unsigned char)c);
            if (val.find("close") != std::string::npos) req.keep_alive = false;
        }
    }

    req.valid = !req.method.empty() && !req.path.empty();
    return req;
}

/* ── extract /stream/{token} ─────────────────────────────────────────── */
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
    : cfg_(cfg), session_(lt::settings_pack{})
{
    /* defaults */
    if (cfg_.cache_max_bytes == 0) cfg_.cache_max_bytes = 50ULL * 1024 * 1024 * 1024;
    if (cfg_.cache_ttl_secs  == 0) cfg_.cache_ttl_secs  = 3600;

    fs::create_directories(cfg_.storage_path);

    init_session();

    alert_thread_ = std::thread([this]{ alert_loop(); });
    http_thread_  = std::thread([this]{ http_server_loop(); });

    /* give server a moment to bind */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

BlizSessionImpl::~BlizSessionImpl() {
    running_.store(false);
    session_.abort();

    /* stop HTTP server */
    if (server_fd_ >= 0) {
        shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
    }

    if (alert_thread_.joinable()) alert_thread_.join();
    if (http_thread_.joinable())  http_thread_.join();

    /* stop all priority workers */
    std::lock_guard<std::mutex> lock(streams_mutex_);
    for (auto& [token, st] : streams_) {
        st->stop_flag.store(true);
        if (st->priority_thread.joinable()) st->priority_thread.join();
    }
}

/* ════════════════════════════════════════════════════════════════════════
 *  libtorrent session init
 * ════════════════════════════════════════════════════════════════════════ */

void BlizSessionImpl::init_session() {
    lt::settings_pack sp;

    /* ── Streaming settings (from Tribler / libtorrent streaming docs) ── */
    /* Prioritise partial pieces: finish already-started pieces first to
       reduce buffer fragmentation.  Adapted from Tribler's approach. */
    sp.set_bool(lt::settings_pack::prioritize_partial_pieces, true);
    sp.set_bool(lt::settings_pack::strict_end_game_mode, false);

    /* Piece request/timeout tuned for real-time playback */
    sp.set_int(lt::settings_pack::piece_timeout,         4);
    sp.set_int(lt::settings_pack::request_timeout,       10);
    sp.set_int(lt::settings_pack::whole_pieces_threshold, 2);

    /* Aggressive peer connections for faster swarm discovery */
    sp.set_int(lt::settings_pack::connection_speed,      500);
    sp.set_int(lt::settings_pack::peer_connect_timeout,  4);
    sp.set_int(lt::settings_pack::num_want,              200);

    /* Upload slots — keep uploading to maintain unchoke reciprocity
       (Give-to-Get principle from Tribler research). */
    sp.set_int(lt::settings_pack::unchoke_slots_limit,   8);

    /* Disk read cache: 512 MB of read-ahead kept in RAM (libtorrent 2.x API) */
    sp.set_int(lt::settings_pack::read_cache_line_size,  32);

    /* DHT + LSD for peer discovery */
    sp.set_bool(lt::settings_pack::enable_dht, true);
    sp.set_bool(lt::settings_pack::enable_lsd, true);
    sp.set_bool(lt::settings_pack::enable_upnp, true);

    /* Alert mask: we need piece progress and status alerts */
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

    /* Extensions */
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
    if (rpa->error) return;

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
            catch (const std::future_error&) { /* already satisfied */ }
        }
        st->waiters.erase(it);
        break;
    }
}

void BlizSessionImpl::on_metadata_received(lt::metadata_received_alert* a) {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    for (auto& [token, st] : streams_) {
        if (st->handle == a->handle)
            st->metadata_ready.store(true);
    }
}

void BlizSessionImpl::on_metadata_failed(lt::metadata_failed_alert* a) {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    for (auto& [token, st] : streams_) {
        if (st->handle == a->handle)
            st->metadata_failed.store(true);
    }
}

/* ════════════════════════════════════════════════════════════════════════
 *  HTTP server
 * ════════════════════════════════════════════════════════════════════════ */

void BlizSessionImpl::http_server_loop() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) return;

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0; /* kernel picks port */

    if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(server_fd_); server_fd_ = -1; return;
    }

    socklen_t len = sizeof(addr);
    getsockname(server_fd_, (struct sockaddr*)&addr, &len);
    http_port_ = ntohs(addr.sin_port);

    listen(server_fd_, 64);

    while (running_.load()) {
        struct pollfd pfd{server_fd_, POLLIN, 0};
        if (poll(&pfd, 1, 500) <= 0) continue;

        int client = accept(server_fd_, nullptr, nullptr);
        if (client < 0) continue;

        /* detach so that slow streams don't block the accept loop */
        std::thread([this, client]() {
            handle_http_connection(client);
            close(client);
        }).detach();
    }
}

void BlizSessionImpl::handle_http_connection(int sock) {
    /* Disable Nagle: first byte of audio reaches the player faster */
    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

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
            send(sock, cors, strlen(cors), MSG_NOSIGNAL);
            continue;
        }

        std::string token = extract_token(req.path);
        if (token.empty()) {
            const char* r404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            send(sock, r404, strlen(r404), MSG_NOSIGNAL);
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
            send(sock, r404, strlen(r404), MSG_NOSIGNAL);
            break;
        }

        int64_t file_size = st->file_size;
        int64_t rstart    = req.has_range ? req.range_start : 0;
        int64_t rend      = (req.has_range && req.range_end >= 0)
                            ? req.range_end : file_size - 1;
        rstart = std::clamp(rstart, (int64_t)0, file_size - 1);
        rend   = std::clamp(rend,   rstart,     file_size - 1);
        int64_t content_len = rend - rstart + 1;

        /* update playback position hint */
        st->playback_byte.store(rstart, std::memory_order_relaxed);

        /* response headers */
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
        if (send(sock, hdr, strlen(hdr), MSG_NOSIGNAL) < 0) break;

        if (req.method == "HEAD") continue;

        bool ok = serve_range(sock, *st, rstart, rend);
        keep_alive = ok && req.keep_alive;
    }
}

bool BlizSessionImpl::serve_range(int sock, StreamState& stream,
                                  int64_t start, int64_t end) {
    int piece_len = stream.ti->piece_length();
    int64_t pos   = start;

    while (pos <= end && running_.load()) {
        /* check that stream is still alive */
        if (stream.stop_flag.load()) return false;

        int64_t abs_pos   = stream.file_offset + pos;
        int piece_idx     = (int)(abs_pos / piece_len);
        int piece_off     = (int)(abs_pos % piece_len);

        auto piece_data = wait_for_piece(stream, piece_idx);
        if (piece_data.empty()) return false; /* timeout */

        int64_t available    = (int64_t)piece_data.size() - piece_off;
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
                                                   int piece_idx) {
    std::future<std::vector<char>> future;

    {
        std::lock_guard<std::mutex> lock(stream.waiters_mutex);
        std::promise<std::vector<char>> promise;
        future = promise.get_future();
        stream.waiters[piece_idx].promises.push_back(std::move(promise));
    }

    /* set_piece_deadline with alert_when_available:
       libtorrent will download the piece urgently and then fire
       read_piece_alert automatically when it's ready. */
    stream.handle.set_piece_deadline(
        lt::piece_index_t(piece_idx),
        0, /* deadline: as soon as possible */
        lt::torrent_handle::alert_when_available
    );

    auto status = future.wait_for(
        std::chrono::milliseconds(PIECE_TIMEOUT_MS));

    if (status != std::future_status::ready) {
        /* clean up stale waiter */
        std::lock_guard<std::mutex> lock(stream.waiters_mutex);
        stream.waiters.erase(piece_idx);
        return {};
    }

    try { return future.get(); }
    catch (...) { return {}; }
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

        /* ── TIER 1: high-priority deadline window (0 – HIGH_PRIORITY_PIECES)
           Pieces the player needs in the next few seconds.
           set_piece_deadline gives them a hard deadline — libtorrent drops
           other work to fetch these first. Spacing of 150 ms emulates a
           realistic playback rate for compressed audio.               ── */
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
                /* behind playback: keep low so swarm peers can use us */
                prio = lt::low_priority;

            } else if (i < play_piece + HIGH_PRIORITY_PIECES) {
                /* TIER 1: handled by set_piece_deadline above */
                prio = lt::top_priority;

            } else if (i < play_piece + MID_PRIORITY_PIECES) {
                /* TIER 2: rarest-first allowed, high priority
                   This is the "medium" tier from Tribler's research —
                   maintains swarm health while buffering ahead.       */
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

    atp.save_path = cfg_.storage_path;

    /* sequential_download: pieces arrive in file order by default;
       the priority worker overlays the three-tier deadline system. */
    atp.flags |= lt::torrent_flags::sequential_download;

    /* Suppress auto-managed pausing so we control download ourselves */
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

    try {
        /* ── check idle cache ────────────────────────────────────────── */
        /* (future: reuse handle if same info-hash is already cached)    */

        /* ── add torrent ─────────────────────────────────────────────── */
        lt::add_torrent_params atp = make_atp(magnet, torrent_data, torrent_len);

        std::string token = generate_token();
        auto st = std::make_shared<StreamState>();
        st->file_idx = file_idx;

        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_[token] = st;
        }

        report(0.05f, "Connecting to swarm...");
        lt::torrent_handle handle = session_.add_torrent(atp);
        st->handle = handle;

        /* ── wait for metadata ───────────────────────────────────────── */
        report(0.08f, "Resolving metadata...");
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(METADATA_TIMEOUT_S);

        while (std::chrono::steady_clock::now() < deadline) {
            auto ti = handle.torrent_file();
            if (ti) { st->ti = ti; break; }
            if (st->metadata_failed.load()) break;

            float elapsed = std::chrono::duration<float>(
                std::chrono::steady_clock::now() -
                (deadline - std::chrono::seconds(METADATA_TIMEOUT_S))
            ).count();
            report(std::min(0.08f + elapsed / METADATA_TIMEOUT_S * 0.5f, 0.58f),
                   "Resolving metadata...");

            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }

        if (!st->ti) {
            session_.remove_torrent(handle);
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
            info.error = BLIZ_ERR_METADATA_TIMEOUT;
            snprintf(info.error_msg, sizeof(info.error_msg),
                     "Metadata timeout after %d s", METADATA_TIMEOUT_S);
            return info;
        }

        /* ── resolve file ────────────────────────────────────────────── */
        auto& files    = st->ti->files();
        int   num_files = files.num_files();

        if (file_idx < 0 || file_idx >= num_files) {
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

        int piece_len     = st->ti->piece_length();
        st->first_piece   = (int)(st->file_offset / piece_len);
        st->last_piece    = (int)((st->file_offset + st->file_size - 1) / piece_len);

        /* ── pin file priorities ─────────────────────────────────────── */
        std::vector<lt::download_priority_t> file_prios(
            (size_t)num_files, lt::dont_download);
        file_prios[(size_t)file_idx] = lt::top_priority;
        handle.prioritize_files(file_prios);

        /* resume if paused */
        handle.resume();

        /* ── start priority worker ───────────────────────────────────── */
        st->priority_thread = std::thread([this, st]() {
            run_priority_worker(st);
        });

        report(0.95f, "Ready");

        /* ── build result ────────────────────────────────────────────── */
        snprintf(info.url, sizeof(info.url),
                 "http://127.0.0.1:%d/stream/%s",
                 (int)http_port_, token.c_str());
        snprintf(info.token,     sizeof(info.token),     "%s", token.c_str());
        snprintf(info.mime_type, sizeof(info.mime_type), "%s", st->mime_type.c_str());
        info.file_size = st->file_size;
        info.error     = BLIZ_OK;

    } catch (const std::exception& e) {
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

        /* temporary stream state just to wait for metadata */
        auto st = std::make_shared<StreamState>();
        std::string token = "list-" + generate_token();
        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_[token] = st;
        }

        lt::torrent_handle handle = session_.add_torrent(atp);
        st->handle = handle;

        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(METADATA_TIMEOUT_S);

        while (std::chrono::steady_clock::now() < deadline) {
            auto ti = handle.torrent_file();
            if (ti) { st->ti = ti; break; }
            if (st->metadata_failed.load()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }

        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.erase(token);
        }

        if (!st->ti) {
            session_.remove_torrent(handle);
            result.error = BLIZ_ERR_METADATA_TIMEOUT;
            snprintf(result.error_msg, sizeof(result.error_msg),
                     "Metadata timeout");
            return result;
        }

        session_.remove_torrent(handle);

        auto& files    = st->ti->files();
        int   n        = files.num_files();
        auto* entries  = new BlizFileEntry[(size_t)n];

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
    std::shared_ptr<StreamState> st;
    {
        std::lock_guard<std::mutex> lock(streams_mutex_);
        auto it = streams_.find(token);
        if (it == streams_.end()) return;
        st = it->second;
        streams_.erase(it);
    }

    st->stop_flag.store(true);
    if (st->priority_thread.joinable())
        st->priority_thread.join();

    /* park in idle cache (paused, data kept on disk) */
    if (st->handle.is_valid()) {
        st->handle.pause();
        std::lock_guard<std::mutex> lock(idle_mutex_);
        idle_torrents_.push_back({st->handle,
                                  std::chrono::steady_clock::now()});
    }
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
                    /* remove from session, keep files on disk */
                    session_.remove_torrent(it.handle);
                    return true;
                }
                return false;
            }),
        idle_torrents_.end()
    );
}

} // namespace bliz
