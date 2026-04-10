/**
 * blizorukost — api.cpp
 * Thin C wrapper around BlizSessionImpl.
 * All exceptions are caught here so nothing crosses the FFI boundary.
 */

#include "session.hpp"
#include <cstdlib>
#include <cstring>
#include <new>

using namespace bliz;

/* ── BlizSession is just an alias for the impl class ─────────────────── */
struct BlizSession : public BlizSessionImpl {
    using BlizSessionImpl::BlizSessionImpl;
};

/* ════════════════════════════════════════════════════════════════════════
 *  Session lifecycle
 * ════════════════════════════════════════════════════════════════════════ */

BlizSession* bliz_session_create(const BlizConfig* config) {
    if (!config) return nullptr;
    try {
        return new BlizSession(*config);
    } catch (...) {
        return nullptr;
    }
}

void bliz_session_destroy(BlizSession* session) {
    delete session;
}

/* ════════════════════════════════════════════════════════════════════════
 *  Streaming
 * ════════════════════════════════════════════════════════════════════════ */

BlizStreamInfo bliz_stream_prepare(BlizSession*   session,
                                    const char*    magnet,
                                    const uint8_t* torrent_data,
                                    size_t         torrent_len,
                                    int            file_idx,
                                    BlizProgressFn progress_fn,
                                    void*          userdata) {
    BlizStreamInfo info{};
    if (!session) {
        info.error = BLIZ_ERR_BAD_INPUT;
        snprintf(info.error_msg, sizeof(info.error_msg), "null session");
        return info;
    }
    try {
        return session->prepare(magnet, torrent_data, torrent_len,
                                file_idx, progress_fn, userdata);
    } catch (const std::exception& e) {
        info.error = BLIZ_ERR_INTERNAL;
        snprintf(info.error_msg, sizeof(info.error_msg), "%s", e.what());
        return info;
    }
}

void bliz_stream_notify_position(BlizSession* session,
                                  const char*  token,
                                  int64_t      byte_offset) {
    if (!session || !token) return;
    try { session->notify_position(token, byte_offset); }
    catch (...) {}
}

void bliz_stream_release(BlizSession* session, const char* token) {
    if (!session || !token) return;
    try { session->release_stream(token); }
    catch (...) {}
}

/* ════════════════════════════════════════════════════════════════════════
 *  File listing
 * ════════════════════════════════════════════════════════════════════════ */

BlizFileList bliz_list_files(BlizSession*   session,
                              const char*    magnet,
                              const uint8_t* torrent_data,
                              size_t         torrent_len) {
    BlizFileList result{};
    if (!session) {
        result.error = BLIZ_ERR_BAD_INPUT;
        snprintf(result.error_msg, sizeof(result.error_msg), "null session");
        return result;
    }
    try {
        return session->list_files(magnet, torrent_data, torrent_len);
    } catch (const std::exception& e) {
        result.error = BLIZ_ERR_INTERNAL;
        snprintf(result.error_msg, sizeof(result.error_msg), "%s", e.what());
        return result;
    }
}

void bliz_file_list_free(BlizFileList* list) {
    if (!list) return;
    delete[] list->files;
    list->files = nullptr;
    list->count = 0;
}

/* ════════════════════════════════════════════════════════════════════════
 *  Cache / misc
 * ════════════════════════════════════════════════════════════════════════ */

void bliz_session_evict(BlizSession* session) {
    if (!session) return;
    try { session->evict(); }
    catch (...) {}
}

uint16_t bliz_session_http_port(BlizSession* session) {
    if (!session) return 0;
    return session->http_port();
}
