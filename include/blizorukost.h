/**
 * blizorukost — libtorrent-based streaming engine for neegde
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
typedef struct BlizSession BlizSession;

/* ── Configuration ─────────────────────────────────────────────────────── */
typedef struct {
    const char* storage_path;    /* directory for torrent data              */
    uint64_t    cache_max_bytes; /* max disk usage; 0 = 50 GB              */
    uint32_t    cache_ttl_secs;  /* idle-torrent TTL; 0 = 3 600 s          */
    int         listen_port;     /* BT listen port; 0 = random             */
} BlizConfig;

/* ── Error codes ───────────────────────────────────────────────────────── */
typedef enum {
    BLIZ_OK                  = 0,
    BLIZ_ERR_METADATA_TIMEOUT = 1,
    BLIZ_ERR_INVALID_FILE    = 2,
    BLIZ_ERR_BAD_INPUT       = 3,
    BLIZ_ERR_INTERNAL        = 99,
} BlizError;

/* ── Stream result ─────────────────────────────────────────────────────── */
typedef struct {
    char      url[512];       /* http://127.0.0.1:PORT/stream/TOKEN        */
    char      token[128];     /* opaque stream identifier                  */
    int64_t   file_size;      /* bytes                                     */
    char      mime_type[64];  /* e.g. "audio/flac"                         */
    BlizError error;
    char      error_msg[256];
} BlizStreamInfo;

/* ── File entry (for magnet file listing) ──────────────────────────────── */
typedef struct {
    char    name[512];
    char    mime[64];
    int64_t size;
    int     index;            /* file index within the torrent             */
} BlizFileEntry;

typedef struct {
    BlizFileEntry* files;
    int            count;
    BlizError      error;
    char           error_msg[256];
} BlizFileList;

/* ── Progress callback ─────────────────────────────────────────────────── */
typedef void (*BlizProgressFn)(float progress,       /* 0.0 – 1.0         */
                               const char* status,
                               void*       userdata);

/* ── Session lifecycle ─────────────────────────────────────────────────── */
BlizSession* bliz_session_create(const BlizConfig* config);
void         bliz_session_destroy(BlizSession* session);

/* ── Streaming ─────────────────────────────────────────────────────────── */

/**
 * Prepare a stream.  Blocks until metadata is resolved (or timeout).
 * On success returns a URL that any HTTP range-request client can open.
 *
 * @param magnet       Magnet URI string (may be NULL if torrent_data given)
 * @param torrent_data Raw .torrent bytes decoded from base64 (may be NULL)
 * @param torrent_len  Length of torrent_data
 * @param file_idx     Which file inside the torrent to stream
 * @param progress_fn  Called periodically during metadata resolution
 * @param userdata     Passed verbatim to progress_fn
 */
BlizStreamInfo bliz_stream_prepare(
    BlizSession*   session,
    const char*    magnet,
    const uint8_t* torrent_data,
    size_t         torrent_len,
    int            file_idx,
    BlizProgressFn progress_fn,
    void*          userdata
);

/**
 * Notify the engine of the current playback byte offset.
 * Call this whenever the media player position changes (e.g. seek).
 * The priority worker uses this to slide the high-priority window.
 */
void bliz_stream_notify_position(BlizSession*  session,
                                 const char*   token,
                                 int64_t       byte_offset);

/**
 * Release a stream token.  The torrent stays cached on disk.
 * Any ongoing HTTP connection to this token will be closed.
 */
void bliz_stream_release(BlizSession* session, const char* token);

/* ── File listing ──────────────────────────────────────────────────────── */

/**
 * Resolve metadata and return the file list.
 * Call bliz_file_list_free() when done.
 */
BlizFileList bliz_list_files(
    BlizSession*   session,
    const char*    magnet,
    const uint8_t* torrent_data,
    size_t         torrent_len
);

void bliz_file_list_free(BlizFileList* list);

/* ── Cache management ──────────────────────────────────────────────────── */
/** Remove from session all idle (released) torrents; leave disk data. */
void bliz_session_evict(BlizSession* session);

/** HTTP port the internal server is listening on. */
uint16_t bliz_session_http_port(BlizSession* session);

#ifdef __cplusplus
}
#endif
