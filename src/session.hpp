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

#include "compat.hpp"
#include "../include/vozduxan.h"

namespace vozduxan {

namespace lt = libtorrent;

/* ── Tuning constants ──────────────────────────────────────────────────── */
constexpr int HIGH_PRIORITY_PIECES  = 20;    /* pieces with hard deadline  */
constexpr int MID_PRIORITY_PIECES   = 60;    /* high-priority lookahead    */
constexpr int PRIORITY_TICK_MS      = 100;   /* priority worker interval   */
constexpr int PIECE_TIMEOUT_MS      = 20'000;/* max wait for one piece     */
constexpr int METADATA_TIMEOUT_S    = 90;    /* magnet metadata resolution (slow DHT / trackers) */
constexpr int HTTP_RECV_TIMEOUT_MS  = 5'000; /* idle HTTP socket           */
constexpr int FAST_START_TIMEOUT_MS = 8'000; /* max wait for first piece   */

/* ── Per-piece waiting ─────────────────────────────────────────────────── */
struct PieceWaiters {
    std::vector<std::promise<std::vector<char>>> promises;
};

/* ── Per-stream state ──────────────────────────────────────────────────── */
struct StreamState {
    lt::torrent_handle                      handle;
    std::shared_ptr<const lt::torrent_info> ti;

    int     file_idx    = 0;
    int64_t file_offset = 0;
    int64_t file_size   = 0;
    int     first_piece = 0;
    int     last_piece  = 0;

    std::string mime_type;

    std::atomic<int64_t>  playback_byte{0};
    std::atomic<bool>     stop_flag{false};

    /* metadata signalling */
    std::atomic<bool>     metadata_ready{false};
    std::atomic<bool>     metadata_failed{false};
    std::mutex            metadata_mtx;
    std::condition_variable metadata_cv;

    /* seek generation: increment to cancel in-flight serve_range calls */
    std::atomic<uint64_t> seek_generation{0};

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
class VozduxanSessionImpl {
public:
    explicit VozduxanSessionImpl(const VozduxanConfig& cfg);
    ~VozduxanSessionImpl();

    VozduxanStreamInfo prepare(const char*    magnet,
                           const uint8_t* torrent_data,
                           size_t         torrent_len,
                           int            file_idx,
                           VozduxanProgressFn progress_fn,
                           void*          userdata);

    VozduxanFileList list_files(const char*    magnet,
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
    void handle_http_connection(sock_t sock);
    bool serve_range(sock_t sock, StreamState& stream,
                     int64_t start, int64_t end, uint64_t gen);

    /* piece read synchronisation */
    std::vector<char> wait_for_piece(StreamState& stream, int piece_idx,
                                     uint64_t seek_gen);

    /* piece priority worker (runs in its own thread per stream) */
    void run_priority_worker(std::shared_ptr<StreamState> stream);

    /* helpers */
    lt::add_torrent_params make_atp(const char*    magnet,
                                    const uint8_t* torrent_data,
                                    size_t         torrent_len);
    std::string    generate_token();
    static std::string detect_mime(const std::string& filename);

    /* structured logging: log_fn_ when set; else stderr */
    void log(const char* fmt, ...);

    /* ── members ─────────────────────────────────────────────────────── */
    /* storage_path_ owns the path string so cfg_.storage_path doesn't
       dangle after the caller's CString is dropped on the Rust side. */
    std::string storage_path_;
    VozduxanConfig  cfg_;
    lt::session session_;
    uint16_t    http_port_{0};
    sock_t      server_fd_{kInvalidSock};

    std::atomic<bool> running_{true};

    std::thread alert_thread_;
    std::thread http_thread_;

    std::mutex streams_mutex_;
    std::unordered_map<std::string, std::shared_ptr<StreamState>> streams_;

    /* idle torrents (released but cached) */
    std::mutex               idle_mutex_;
    std::vector<IdleTorrent> idle_torrents_;

    std::atomic<uint32_t> token_counter_{0};

    VozduxanLogFn   log_fn_{nullptr};
    void*       log_userdata_{nullptr};
};

} // namespace vozduxan
