/**
 * vozduxan — api.cpp
 * Thin C wrapper around VozduxanSessionImpl.
 * All exceptions are caught here so nothing crosses the FFI boundary.
 */

#include "session.hpp"
#include <cstdlib>
#include <cstring>
#include <new>

using namespace vozduxan;

struct VozduxanSession : public VozduxanSessionImpl {
    using VozduxanSessionImpl::VozduxanSessionImpl;
};

/* ════════════════════════════════════════════════════════════════════════
 *  Session lifecycle
 * ════════════════════════════════════════════════════════════════════════ */

VozduxanSession* vozduxan_session_create(const VozduxanConfig* config) {
    if (!config) return nullptr;
    try {
        return new VozduxanSession(*config);
    } catch (...) {
        return nullptr;
    }
}

void vozduxan_session_destroy(VozduxanSession* session) {
    delete session;
}

/* ════════════════════════════════════════════════════════════════════════
 *  Streaming
 * ════════════════════════════════════════════════════════════════════════ */

VozduxanStreamInfo vozduxan_stream_prepare(VozduxanSession*   session,
                                    const char*    magnet,
                                    const uint8_t* torrent_data,
                                    size_t         torrent_len,
                                    int            file_idx,
                                    int            is_main,
                                    VozduxanProgressFn progress_fn,
                                    void*          userdata) {
    VozduxanStreamInfo info{};
    if (!session) {
        info.error = VOZDUXAN_ERR_BAD_INPUT;
        snprintf(info.error_msg, sizeof(info.error_msg), "null session");
        return info;
    }
    try {
        return session->prepare(magnet, torrent_data, torrent_len,
                                file_idx, is_main, progress_fn, userdata);
    } catch (const std::exception& e) {
        info.error = VOZDUXAN_ERR_INTERNAL;
        snprintf(info.error_msg, sizeof(info.error_msg), "%s", e.what());
        return info;
    }
}

VozduxanStreamStats vozduxan_stream_stats(VozduxanSession* session,
                                          const char*  token) {
    VozduxanStreamStats result{0, 0};
    if (!session || !token) return result;
    try {
        auto s = session->stream_stats(token);
        result.download_rate_bytes = s.download_rate_bytes;
        result.num_peers           = s.num_peers;
    } catch (...) {}
    return result;
}

void vozduxan_stream_notify_position(VozduxanSession* session,
                                  const char*  token,
                                  int64_t      byte_offset) {
    if (!session || !token) return;
    try { session->notify_position(token, byte_offset); }
    catch (...) {}
}

void vozduxan_stream_release(VozduxanSession* session, const char* token) {
    if (!session || !token) return;
    try { session->release_stream(token); }
    catch (...) {}
}

/* ════════════════════════════════════════════════════════════════════════
 *  File listing
 * ════════════════════════════════════════════════════════════════════════ */

VozduxanFileList vozduxan_list_files(VozduxanSession*   session,
                              const char*    magnet,
                              const uint8_t* torrent_data,
                              size_t         torrent_len) {
    VozduxanFileList result{};
    if (!session) {
        result.error = VOZDUXAN_ERR_BAD_INPUT;
        snprintf(result.error_msg, sizeof(result.error_msg), "null session");
        return result;
    }
    try {
        return session->list_files(magnet, torrent_data, torrent_len);
    } catch (const std::exception& e) {
        result.error = VOZDUXAN_ERR_INTERNAL;
        snprintf(result.error_msg, sizeof(result.error_msg), "%s", e.what());
        return result;
    }
}

void vozduxan_file_list_free(VozduxanFileList* list) {
    if (!list) return;
    delete[] list->files;
    list->files = nullptr;
    list->count = 0;
}

/* ════════════════════════════════════════════════════════════════════════
 *  Cache / misc
 * ════════════════════════════════════════════════════════════════════════ */

void vozduxan_session_evict(VozduxanSession* session) {
    if (!session) return;
    try { session->evict(); }
    catch (...) {}
}

uint16_t vozduxan_session_http_port(VozduxanSession* session) {
    if (!session) return 0;
    return session->http_port();
}
