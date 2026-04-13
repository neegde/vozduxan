# vozduxan

C++ static library — libtorrent-based streaming engine for neegde.  
Consumed by neegde-tauri via a C FFI (`include/vozduxan.h`).

## Build

Built automatically by neegde-tauri's `build.rs` via the `cmake` crate.  
To build standalone: `cmake -B build && cmake --build build`.

## Architecture

```
include/vozduxan.h      C API — the only surface visible to Rust
src/session.hpp         VozduxanSessionImpl declaration + StreamState structs
src/session.cpp         All logic: libtorrent session, alert loop, HTTP server
src/api.cpp             Thin C wrappers: VozduxanSession : VozduxanSessionImpl
src/compat.hpp          Cross-platform socket abstractions
```

### Key design points

- **Three-tier piece priority**: TIER1 = `set_piece_deadline()` (20 pieces ahead, time-critical), TIER2 = high priority (20–60 ahead), TIER3 = default (60+). Adapted from Tribler/TU Delft research.
- **Fast-start prebuffer**: after metadata resolves, primes 20 pieces then waits up to 8 s for piece 0 so the HTTP client never blocks on first read.
- **Seek cancellation via `seek_generation`**: each new HTTP Range request increments the counter; `serve_range()` checks it between reads to abort stale transfers instantly.
- **Metadata path**: when `atp.ti` is set (torrent data provided), libtorrent does NOT fire `metadata_received_alert`. `prepare()` checks `handle.torrent_file()` immediately after `add_torrent()` and marks metadata ready without waiting for the alert.
- **Handle-before-register**: `st->handle` is set and metadata checked BEFORE inserting into `streams_` map, avoiding a race where the alert handler can't match the handle.
- **Token format**: `{hex_ms}-{counter}` (e.g. `2e0db8b4-0`).
- **HTTP server**: single-threaded accept loop; each connection gets its own thread; range requests served with chunked reads; stale connections abort on seek via `seek_generation`.

### Log callback

`VozduxanConfig.log_fn` (type `VozduxanLogFn`) receives every `VOZDUXAN_LOG()` line as a formatted string.  
`VozduxanSessionImpl::log()` always writes to stderr; fires the callback when non-null.  
Used by neegde-tauri to route C++ logs into the in-app debug console (`AppDebugLog`).

### Token buckets (three-token design, enforced in Rust)

The C++ side has no concept of token buckets — it just prepares and releases streams.  
The Rust wrapper (`vozduxan_stream.rs`) enforces:
- `current_token` — active playback stream
- `prefetch_token` — background look-ahead for next queue item
- `hover_token` — hover-prefetch; NEVER releases `current_token`

## Common gotchas

- `VozduxanLogFn` must be declared before `VozduxanConfig` in the header (used as a field type).
- `VOZDUXAN_LOG(...)` expands to `this->log(...)` — only valid inside `VozduxanSessionImpl` methods.
- Do not call `vozduxan_log()` as a free function; it no longer exists.
- `VozduxanSession` (in `api.cpp`) publicly inherits `VozduxanSessionImpl` — this is the concrete type returned by `vozduxan_session_create`. Never add virtual functions to `VozduxanSessionImpl` (ABI fragility).
