# blizorukost

> libtorrent-based streaming engine for [neegde](https://github.com/neegde).

**blizorukost** wraps [libtorrent-rasterbar](https://github.com/arvidn/libtorrent) and exposes a
minimal C API that Rust (via FFI) can call from a Tauri application.
It replaces the librqbit-based streaming module in neegde-tauri with a
battle-tested engine that has full support for piece-level prioritisation and
time-critical streaming.

---

## What it does

| Feature | Implementation |
|---|---|
| **Three-tier piece priority** | TIER 1 (0–19 pieces ahead): `set_piece_deadline()` — hard deadline; TIER 2 (20–59): `top_priority`; TIER 3 (60+): `default_priority` |
| **Time-critical mode** | libtorrent's native deadline system ensures the player never stalls waiting for an in-order piece |
| **`prioritize_partial_pieces`** | Enabled globally — unfinished pieces are completed first, reducing buffer fragmentation |
| **HTTP range-request server** | Embedded minimal HTTP/1.1 server on a random localhost port; supports `Range: bytes=` and keep-alive |
| **Adaptive priority window** | Priority worker ticks every 100 ms and slides the deadline window with the playback position reported via `bliz_stream_notify_position()` |
| **Seeking** | HTTP Range requests automatically update `playback_byte`; priority worker recalculates within 100 ms |
| **File listing** | `bliz_list_files()` resolves magnet metadata and returns the file list without starting a stream |
| **Idle cache** | Released torrents are parked (paused, data kept on disk); `bliz_session_evict()` removes TTL-expired ones |

---

## Algorithm credits

The three-tier piece prioritisation strategy and the rationale behind
`prioritize_partial_pieces` are based on streaming research published by the
**Tribler team at TU Delft**:

- Tribler source: <https://github.com/Tribler/tribler> — MIT licence  
  *Johan Pouwelse et al., "P2P Search, Share and Stream", ACM SIGMM 2012*
- libtorrent streaming guide: <https://libtorrent.org/streaming.html>  
  *Arvid Norberg* — BSD licence

---

## Building

### Prerequisites

```bash
# macOS
brew install libtorrent-rasterbar cmake

# Ubuntu / Debian
sudo apt install libtorrent-rasterbar-dev cmake
```

### Standalone

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
# → build/libblizorukost.a
```

### Via neegde-tauri build.rs

The neegde-tauri `build.rs` calls CMake automatically.  You only need
libtorrent installed system-wide; the Rust side handles the rest.

---

## C API

```c
// Session
BlizSession* bliz_session_create(const BlizConfig*);
void         bliz_session_destroy(BlizSession*);

// Streaming (blocking — call from a thread pool)
BlizStreamInfo bliz_stream_prepare(session, magnet, torrent_data, len,
                                   file_idx, progress_fn, userdata);
void bliz_stream_notify_position(session, token, byte_offset);
void bliz_stream_release(session, token);

// File listing
BlizFileList bliz_list_files(session, magnet, torrent_data, len);
void         bliz_file_list_free(BlizFileList*);

// Cache / misc
void     bliz_session_evict(session);
uint16_t bliz_session_http_port(session);
```

See [`include/blizorukost.h`](include/blizorukost.h) for full documentation.

---

## Licence

MIT — see [LICENSE](LICENSE).  
Third-party algorithm credits listed above.
