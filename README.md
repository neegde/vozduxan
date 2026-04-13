# vozduxan

> *воздухан* — зумерский сленг: человек, который много говорит, но ничего не делает.  
> Эта библиотека называется именно так, потому что качает воздух — буквально. Аудио.  
> И в отличие от воздухана — доставляет.

C++ статическая библиотека для стриминга аудио напрямую из торрент-роёв в реальном времени. Написана для [neegde](https://github.com/neegde/neegde-tauri) — десктопного музыкального плеера, который играет треки прямо из торрентов.

---

## Чем отличается от всего остального

Большинство торрент-библиотек построены вокруг одной модели: *скачай → потом используй*. vozduxan эту модель выбрасывает. Она сделана под другой сценарий: пользователь уже нажал play. Он ждёт звук через секунду.

**Трёхуровневая приоритизация кусков** (адаптировано из исследований [Tribler / TU Delft](https://github.com/Tribler/tribler)):

| Уровень | Окно | Режим |
|---------|------|-------|
| 1 | следующие 20 кусков | `set_piece_deadline()` — time-critical, говорим пирам точно когда нужен каждый кусок |
| 2 | куски 21–59 | высокий приоритет, rarest-first разрешён |
| 3 | 60+ вперёд | дефолтный приоритет — здоровье роя, фон |

Скользящее окно следует за позицией воспроизведения. Вызывай `vozduxan_stream_notify_position()` при каждом seek или обновлении прогресса — приоритетный воркер перестроится сам.

**Fast-start пребуфер**: после получения метаданных vozduxan сразу выставляет дедлайны на первые `HIGH_PRIORITY_PIECES` кусков и блокирует `stream_prepare()` до прихода нулевого куска. URL, который он возвращает, готов к чтению с первого байта — никаких зависаний на старте.

**Параллельные Range-запросы не отменяют друг друга.** Браузеры и медиаплееры обычно открывают два конкурентных Range-запроса при снифе файла (один за заголовками, один за данными). Счётчик `seek_generation` в vozduxan прерывает `serve_range` только при *освобождении стрима*, а не при открытии нового Range. На это ушло время.

**Hover-prefetch из коробки.** Библиотека не знает ни о каком «ховере» — она просто готовит стримы и выдаёт токены. Но она спроектирована так, что можно вызвать `stream_prepare()` заранее, удержать токен, и если пользователь нажмёт play — стрим уже готов. Не нажал — вызвал `stream_release()`. Ресурсы не тратятся.

**Тонкие раздачи всё равно находят пиров.** Восемь открытых UDP-трекеров автоматически добавляются в каждый `add_torrent_params`, состояние DHT сохраняется на диск между сессиями, и `force_reannounce(0)` срабатывает сразу после добавления торрента. Старые релизы с тремя сидами — тоже находятся.

---

## Как это работает

vozduxan запускает libtorrent-сессию и встроенный HTTP-сервер в фоновых потоках. При вызове `stream_prepare()`:

1. Метаданные торрента резолвятся (из байт `.torrent` если переданы, или через DHT/трекеры из магнета)
2. Первые куски получают дедлайны, окно приоритетов выставляется
3. Сервер ждёт нулевой кусок
4. Возвращает `VozduxanStreamInfo` с локальным URL `http://127.0.0.1:PORT/stream/TOKEN`

URL поддерживает HTTP `Range`-запросы — любой `<audio src>`, `AVPlayer`, `mpv` или что угодно, умеющее HTTP, может свободно перематывать. Приоритетное окно следует за позицией.

---

## API

Чистый C API — легко биндится из Rust, Python, Go, откуда угодно.

```c
// Запуск сессии
VozduxanConfig cfg = {
    .storage_path    = "/tmp/vozduxan-cache",
    .cache_max_bytes = 50ULL * 1024 * 1024 * 1024,
    .cache_ttl_secs  = 3600,
    .listen_port     = 0,        // случайный порт
    .log_fn          = my_log_callback,
    .log_userdata    = ctx,
};
VozduxanSession* session = vozduxan_session_create(&cfg);

// Подготовка стрима (блокирует до готовности первого куска)
VozduxanStreamInfo info = vozduxan_stream_prepare(
    session,
    "magnet:?xt=urn:btih:...",  // magnet URI
    NULL, 0,                     // или: сырые байты .torrent
    2,                           // индекс файла внутри торрента
    on_progress, ctx
);

if (info.error == VOZDUXAN_OK) {
    // info.url → "http://127.0.0.1:PORT/stream/TOKEN"
    // отдаём медиаплееру
}

// При обновлении позиции / seek
vozduxan_stream_notify_position(session, info.token, byte_offset);

// Трек закончился / пользователь переключил
vozduxan_stream_release(session, info.token);

// Выключение
vozduxan_session_destroy(session);
```

### Коды ошибок

| Код | Значение |
|-----|---------|
| `VOZDUXAN_OK` | Всё хорошо |
| `VOZDUXAN_ERR_METADATA_TIMEOUT` | Не удалось получить метаданные торрента вовремя |
| `VOZDUXAN_ERR_INVALID_FILE` | `file_idx` вне диапазона или файл не аудио |
| `VOZDUXAN_ERR_BAD_INPUT` | Не передан ни магнет, ни торрент |
| `VOZDUXAN_ERR_INTERNAL` | Что-то пошло не так внутри |

---

## Сборка

Нужны **CMake 3.20+** и **C++17**.

### macOS / Linux (системный libtorrent)

```bash
# macOS
brew install libtorrent-rasterbar

# Ubuntu / Debian
sudo apt-get install libtorrent-rasterbar-dev

cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Windows

```bash
vcpkg install libtorrent:x64-windows-static
cmake -B build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_TARGET_TRIPLET=x64-windows-static
cmake --build build --config Release
```

### Нет системного libtorrent?

CMake упадёт на FetchContent (libtorrent v2.0.10 из исходников). Работает везде, но первая сборка долгая — особенно на Windows, где ещё тянется Boost. Налей кофе.

---

## Использование как сабмодуль

```bash
git submodule add https://github.com/neegde/vozduxan.git vozduxan
```

```cmake
add_subdirectory(vozduxan)
target_link_libraries(your_target PRIVATE vozduxan)
```

CMake найдёт libtorrent или скачает сам.

---

## Rust-биндинги

vozduxan — стриминговый бэкенд для [neegde-tauri](https://github.com/neegde/neegde-tauri). Rust-слой живёт в `src-tauri/src/vozduxan_ffi.rs` (сырой FFI) и `vozduxan_stream.rs` (безопасная обёртка + Tauri команды). Рабочий референс если делаешь что-то похожее.

---

## Благодарности

Алгоритм приоритизации кусков адаптирован из [Tribler](https://github.com/Tribler/tribler) (TU Delft, MIT).  
Построено на [libtorrent-rasterbar](https://libtorrent.org) by Arvid Norberg (BSD).

---
---

# vozduxan

> *vozdukhan* — Gen Z Russian slang: someone who talks a lot but never delivers.  
> This library is named that way because it pumps air — literally. Audio.  
> And unlike the vozdukhan — it actually delivers.

C++ static library for real-time BitTorrent audio streaming. Built for [neegde](https://github.com/neegde/neegde-tauri) — a desktop music player that streams directly from torrent swarms.

---

## What makes this different

Most torrent libraries are built around one mental model: *download → then use*. vozduxan throws that out. It's designed around a different idea: the user already clicked play. They expect audio in under a second.

**Three-tier piece prioritization** (adapted from [Tribler / TU Delft](https://github.com/Tribler/tribler) streaming research):

| Tier | Window | Mode |
|------|--------|------|
| 1 | next 20 pieces | `set_piece_deadline()` — time-critical, tells peers exactly when you need each piece |
| 2 | pieces 21–59 | high priority, rarest-first allowed |
| 3 | 60+ ahead | default priority — swarm health, background |

This sliding window follows your playback position. Call `vozduxan_stream_notify_position()` on every seek or progress tick and the priority worker adjusts automatically.

**Fast-start prebuffer**: after metadata resolves, vozduxan immediately deadlines the first `HIGH_PRIORITY_PIECES` pieces and blocks `stream_prepare()` until piece 0 arrives. The URL it returns is ready to read on byte zero — no stall on first HTTP read.

**Parallel range requests don't cancel each other.** Browsers and media players typically fire two concurrent range requests when they sniff a file (one for the headers, one to start reading). vozduxan's `seek_generation` counter only aborts `serve_range` when the *stream is released*, not when another range opens. Took a while to figure that one out.

**Hover-prefetch friendly.** The library has no concept of "hover" itself — it just prepares streams and gives you tokens. But it's designed so you can call `stream_prepare()` speculatively, hold the token, and if the user actually clicks play — you already have it. If they don't — call `stream_release()`. Zero wasted resources.

**Thin-seeded torrents actually connect.** Eight open UDP trackers are injected into every `add_torrent_params` automatically, DHT state is persisted to disk between sessions, and `force_reannounce(0)` fires right after adding a torrent. Old releases sitting with 3 seeders can still be found.

---

## How it works

vozduxan runs a libtorrent session and an embedded HTTP server in background threads. When you call `stream_prepare()`:

1. Torrent metadata is resolved (from `.torrent` bytes if provided, or via DHT/trackers from magnet)
2. First pieces are deadlined, priority window is set
3. Server waits for piece 0
4. Returns a `VozduxanStreamInfo` with a local `http://127.0.0.1:PORT/stream/TOKEN` URL

That URL supports HTTP `Range` requests, so any `<audio src>`, `AVPlayer`, `mpv`, or anything that speaks HTTP can seek freely. The priority window follows.

---

## API

Plain C API — easy to bind from Rust, Python, Go, wherever.

```c
// Boot a session
VozduxanConfig cfg = {
    .storage_path    = "/tmp/vozduxan-cache",
    .cache_max_bytes = 50ULL * 1024 * 1024 * 1024,
    .cache_ttl_secs  = 3600,
    .listen_port     = 0,        // random
    .log_fn          = my_log_callback,
    .log_userdata    = ctx,
};
VozduxanSession* session = vozduxan_session_create(&cfg);

// Prepare a stream (blocks until first piece is ready)
VozduxanStreamInfo info = vozduxan_stream_prepare(
    session,
    "magnet:?xt=urn:btih:...",  // magnet URI
    NULL, 0,                     // or: raw .torrent bytes
    2,                           // file index inside torrent
    on_progress, ctx
);

if (info.error == VOZDUXAN_OK) {
    // info.url → "http://127.0.0.1:PORT/stream/TOKEN"
    // hand it to your media player
}

// On playback position update / seek
vozduxan_stream_notify_position(session, info.token, byte_offset);

// Done with this track
vozduxan_stream_release(session, info.token);

// Shutdown
vozduxan_session_destroy(session);
```

### Error codes

| Code | Meaning |
|------|---------|
| `VOZDUXAN_OK` | All good |
| `VOZDUXAN_ERR_METADATA_TIMEOUT` | Couldn't resolve torrent metadata in time |
| `VOZDUXAN_ERR_INVALID_FILE` | `file_idx` out of range or not an audio file |
| `VOZDUXAN_ERR_BAD_INPUT` | Neither magnet nor torrent_data provided |
| `VOZDUXAN_ERR_INTERNAL` | Something went wrong internally |

---

## Building

Requires **CMake 3.20+** and **C++17**.

### macOS / Linux (system libtorrent)

```bash
# macOS
brew install libtorrent-rasterbar

# Ubuntu / Debian
sudo apt-get install libtorrent-rasterbar-dev

cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Windows

```bash
vcpkg install libtorrent:x64-windows-static
cmake -B build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_TARGET_TRIPLET=x64-windows-static
cmake --build build --config Release
```

### No system libtorrent?

CMake falls back to FetchContent (libtorrent v2.0.10 from source). Works everywhere but takes a while on first build — especially on Windows where it also pulls Boost. Grab a coffee.

---

## Using as a submodule

```bash
git submodule add https://github.com/neegde/vozduxan.git vozduxan
```

```cmake
add_subdirectory(vozduxan)
target_link_libraries(your_target PRIVATE vozduxan)
```

CMake will find libtorrent or fetch it.

---

## Rust bindings

vozduxan is the streaming backend for [neegde-tauri](https://github.com/neegde/neegde-tauri). The Rust side lives in `src-tauri/src/vozduxan_ffi.rs` (raw FFI) and `vozduxan_stream.rs` (safe wrapper + Tauri commands). Working reference if you're building something similar.

---

## Credits

Piece priority algorithm adapted from [Tribler](https://github.com/Tribler/tribler) (TU Delft, MIT).  
Built on [libtorrent-rasterbar](https://libtorrent.org) by Arvid Norberg (BSD).
