cmake_minimum_required(VERSION 3.20)
project(vozduxan VERSION 1.0.3 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# std::filesystem (libc++) is annotated for macOS 10.15+; align with neegde-tauri build.rs / bundle.
if(APPLE)
    set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15" CACHE STRING "Minimum macOS version" FORCE)
endif()

# ── Homebrew search paths (macOS arm64 / x86_64) ─────────────────────────
if(APPLE)
    foreach(BREW_PREFIX /opt/homebrew /usr/local)
        if(EXISTS "${BREW_PREFIX}")
            list(APPEND CMAKE_PREFIX_PATH "${BREW_PREFIX}")
            list(APPEND CMAKE_INCLUDE_PATH "${BREW_PREFIX}/include")
            list(APPEND CMAKE_LIBRARY_PATH "${BREW_PREFIX}/lib")
        endif()
    endforeach()
endif()

# ── libtorrent-rasterbar ─────────────────────────────────────────────────
find_package(LibtorrentRasterbar CONFIG QUIET)

if(NOT LibtorrentRasterbar_FOUND)
    message(STATUS "[vozduxan] libtorrent not found via CMake; trying pkg-config")
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(LIBTORRENT IMPORTED_TARGET libtorrent-rasterbar)
    endif()

    if(LIBTORRENT_FOUND)
        set(LT_TARGET PkgConfig::LIBTORRENT)
    else()
        message(STATUS "[vozduxan] Fetching libtorrent from GitHub (v2.0.10)")
        include(FetchContent)
        FetchContent_Declare(
            libtorrent
            GIT_REPOSITORY https://github.com/arvidn/libtorrent.git
            GIT_TAG        v2.0.10
            GIT_SHALLOW    TRUE
        )
        set(BUILD_SHARED_LIBS               OFF CACHE BOOL "" FORCE)
        set(deprecated-functions            OFF CACHE BOOL "" FORCE)
        set(CMAKE_POSITION_INDEPENDENT_CODE ON)
        FetchContent_MakeAvailable(libtorrent)
        # In-tree build exposes `torrent-rasterbar` only; the LibtorrentRasterbar::
        # namespaced target exists after install/export, not in FetchContent.
        set(LT_TARGET torrent-rasterbar)
    endif()
else()
    set(LT_TARGET LibtorrentRasterbar::torrent-rasterbar)
endif()

# ── vozduxan static library ───────────────────────────────────────────
add_library(vozduxan STATIC
    src/session.cpp
    src/api.cpp
)

target_include_directories(vozduxan
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(vozduxan PRIVATE ${LT_TARGET})

set_target_properties(vozduxan PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME vozduxan
)

# ── Platform link flags ───────────────────────────────────────────────────
if(WIN32)
    target_link_libraries(vozduxan PRIVATE ws2_32 iphlpapi)
elseif(APPLE)
    target_link_libraries(vozduxan PRIVATE
        "-framework SystemConfiguration"
        "-framework CoreFoundation"
        "-framework IOKit"
    )
elseif(UNIX)
    target_link_libraries(vozduxan PRIVATE pthread)
endif()

# ── Install ───────────────────────────────────────────────────────────────
install(TARGETS vozduxan ARCHIVE DESTINATION lib)
install(FILES include/vozduxan.h DESTINATION include)
