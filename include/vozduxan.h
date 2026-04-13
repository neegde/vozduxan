/**
 * vozduxan — libtorrent-based streaming engine for neegde
 *
 * Implements streaming-optimised BitTorrent using a three-tier piece priority
 * strategy and time-critical mode originally researched by Tribler (TU Delft).
 *
 * Piece priority algorithm adapted from:
 *   Tribler / TU Delft streaming research — https://github.com/Tribler/tribler
 *   Credit: Johan Pouwelse and Tribler team (MIT licence)
 *
 * libtorrent time-critical streaming:
 *   https://libtorrent.org/streaming.html
 *   Credit: Arvid Norberg (BSD licence)
 */

#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handle ─────────────────────────────────────────────────────── */
typedef struct VozduxanSession VozduxanSession;

/* ── Log callback ──────────────────────────────────────────────────────── */
/** Called from any thread; message is a null-terminated UTF-8 string.
 *  NULL disables the callback (logs go to stderr only).                */
typedef void (*VozduxanLogFn)(const char* message, void* userdata);

/* ── Configuration ─────────────────────────────────────────────────────── */
typedef struct {
    const char* storage_path;    /* directory for torrent data              */
    uint64_t    cache_max_bytes; /* max disk usage; 0 = 50 GB              */
    uint32_t    cache_ttl_secs;  /* idle-torrent TTL; 0 = 3 600 s          */
    int         listen_port;     /* BT listen port; 0 = random             */
    VozduxanLogFn   log_fn;          /* optional log callback; NULL = off      */
    void*       log_userdata;    /* passed verbatim to log_fn              */
} VozduxanConfig;

/* ── Error codes ───────────────────────────────────────────────────────── */
typedef enum {
    VOZDUXAN_OK                   = 0,
    VOZDUXAN_ERR_METADATA_TIMEOUT = 1,
    VOZDUXAN_ERR_INVALID_FILE     = 2,
    VOZDUXAN_ERR_BAD_INPUT        = 3,
    VOZDUXAN_ERR_INTERNAL         = 99,
} VozduxanError;

/* ── Stream result ─────────────────────────────────────────────────────── */
typedef struct {
    char      url[512];       /* http://127.0.0.1:PORT/stream/TOKEN        */
    char      token[128];     /* opaque stream identifier                  */
    int64_t   file_size;      /* bytes                                     */
    char      mime_type[64];  /* e.g. "audio/flac"                         */
    VozduxanError error;
    char      error_msg[256];
} VozduxanStreamInfo;

/* ── File entry (for magnet file listing) ──────────────────────────────── */
typedef struct {
    char    name[512];
    char    mime[64];
    int64_t size;
    int     index;            /* file index within the torrent             */
} VozduxanFileEntry;

typedef struct {
    VozduxanFileEntry* files;
    int            count;
    VozduxanError      error;
    char           error_msg[256];
} VozduxanFileList;

/* ── Progress callback ─────────────────────────────────────────────────── */
typedef void (*VozduxanProgressFn)(float progress,       /* 0.0 – 1.0         */
                               const char* status,
                               void*       userdata);

/* ── Session lifecycle ─────────────────────────────────────────────────── */
VozduxanSession* vozduxan_session_create(const VozduxanConfig* config);
void         vozduxan_session_destroy(VozduxanSession* session);

/* ── Streaming ─────────────────────────────────────────────────────────── */

/**
 * Prepare a stream.  Blocks until metadata is resolved and the first
 * audio piece is available (fast-start prebuffer).
 * On success returns a URL that any HTTP range-request client can open.
 *
 * @param magnet       Magnet URI string (may be NULL if torrent_data given)
 * @param torrent_data Raw .torrent bytes (may be NULL if magnet given)
 * @param torrent_len  Length of torrent_data
 * @param file_idx     Which file inside the torrent to stream
 * @param progress_fn  Called periodically during metadata resolution
 * @param userdata     Passed verbatim to progress_fn
 */
VozduxanStreamInfo vozduxan_stream_prepare(
    VozduxanSession*   session,
    const char*    magnet,
    const uint8_t* torrent_data,
    size_t         torrent_len,
    int            file_idx,
    VozduxanProgressFn progress_fn,
    void*          userdata
);

/**
 * Notify the engine of the current playback byte offset.
 * Call on every seek / position update.
 * The priority worker uses this to slide the high-priority window.
 */
void vozduxan_stream_notify_position(VozduxanSession*  session,
                                 const char*   token,
                                 int64_t       byte_offset);

/**
 * Release a stream token.  The torrent stays cached on disk.
 * Any ongoing HTTP connection to this token will be closed.
 */
void vozduxan_stream_release(VozduxanSession* session, const char* token);

/* ── File listing ──────────────────────────────────────────────────────── */

/**
 * Resolve metadata and return the file list.
 * Call vozduxan_file_list_free() when done.
 */
VozduxanFileList vozduxan_list_files(
    VozduxanSession*   session,
    const char*    magnet,
    const uint8_t* torrent_data,
    size_t         torrent_len
);

void vozduxan_file_list_free(VozduxanFileList* list);

/* ── Cache management ──────────────────────────────────────────────────── */

/** Remove from session all idle (released) torrents past TTL; keep disk data. */
void vozduxan_session_evict(VozduxanSession* session);

/** HTTP port the internal server is listening on. */
uint16_t vozduxan_session_http_port(VozduxanSession* session);

#ifdef __cplusplus
}
#endif
