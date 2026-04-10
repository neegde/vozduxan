#pragma once

#include <libtorrent/session.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/torrent_flags.hpp>
#include <libtorrent/download_priority.hpp>
#include <libtorrent/file_storage.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../include/blizorukost.h"

namespace bliz {

namespace lt = libtorrent;

/* ── Tuning constants ──────────────────────────────────────────────────── */
constexpr int HIGH_PRIORITY_PIECES  = 20;    /* pieces with hard deadline  */
constexpr int MID_PRIORITY_PIECES   = 60;    /* high-priority lookahead    */
constexpr int PRIORITY_TICK_MS      = 100;   /* priority worker interval   */
constexpr int PIECE_TIMEOUT_MS      = 20'000;/* wait for one piece         */
constexpr int METADATA_TIMEOUT_S    = 30;    /* magnet metadata resolution */
constexpr int HTTP_RECV_TIMEOUT_MS  = 5'000; /* idle HTTP socket           */

/* ── Per-piece waiting ─────────────────────────────────────────────────── */
struct PieceWaiters {
    std::vector<std::promise<std::vector<char>>> promises;
};

/* ── Per-stream state ──────────────────────────────────────────────────── */
struct StreamState {
    lt::torrent_handle                     handle;
    std::shared_ptr<const lt::torrent_info> ti;

    int     file_idx    = 0;
    int64_t file_offset = 0;
    int64_t file_size   = 0;
    int     first_piece = 0;
    int     last_piece  = 0;

    std::string mime_type;

    std::atomic<int64_t> playback_byte{0};
    std::atomic<bool>    stop_flag{false};
    std::atomic<bool>    metadata_ready{false};
    std::atomic<bool>    metadata_failed{false};

    /* priority worker */
    std::thread priority_thread;

    /* piece read synchronisation */
    std::mutex                              waiters_mutex;
    std::unordered_map<int, PieceWaiters>   waiters;
};

/* ── Idle (released) torrent record ────────────────────────────────────── */
struct IdleTorrent {
    lt::torrent_handle              handle;
    std::chrono::steady_clock::time_point since;
};

/* ── Main session class ────────────────────────────────────────────────── */
class BlizSessionImpl {
public:
    explicit BlizSessionImpl(const BlizConfig& cfg);
    ~BlizSessionImpl();

    BlizStreamInfo prepare(const char*    magnet,
                           const uint8_t* torrent_data,
                           size_t         torrent_len,
                           int            file_idx,
                           BlizProgressFn progress_fn,
                           void*          userdata);

    BlizFileList list_files(const char*    magnet,
                            const uint8_t* torrent_data,
                            size_t         torrent_len);

    void     notify_position(const std::string& token, int64_t byte_offset);
    void     release_stream(const std::string& token);
    void     evict();
    uint16_t http_port() const { return http_port_; }

private:
    /* libtorrent session setup */
    void init_session();

    /* alert processing thread */
    void alert_loop();
    void on_read_piece(lt::read_piece_alert*);
    void on_metadata_received(lt::metadata_received_alert*);
    void on_metadata_failed(lt::metadata_failed_alert*);

    /* HTTP server threads */
    void http_server_loop();
    void handle_http_connection(int sock);
    bool serve_range(int sock, StreamState& stream,
                     int64_t start, int64_t end);

    /* piece priority worker */
    void run_priority_worker(std::shared_ptr<StreamState> stream);

    /* piece read synchronisation */
    std::vector<char> wait_for_piece(StreamState& stream, int piece_idx);

    /* helpers */
    lt::add_torrent_params make_atp(const char*    magnet,
                                    const uint8_t* torrent_data,
                                    size_t         torrent_len);
    std::string    generate_token();
    static std::string detect_mime(const std::string& filename);

    /* ── members ─────────────────────────────────────────────────────── */
    BlizConfig  cfg_;
    lt::session session_;
    uint16_t    http_port_{0};
    int         server_fd_{-1};

    std::atomic<bool> running_{true};

    std::thread alert_thread_;
    std::thread http_thread_;

    std::mutex streams_mutex_;
    std::unordered_map<std::string, std::shared_ptr<StreamState>> streams_;

    /* idle torrents (released but cached) */
    std::mutex               idle_mutex_;
    std::vector<IdleTorrent> idle_torrents_;

    std::atomic<uint32_t> token_counter_{0};
};

} // namespace bliz
