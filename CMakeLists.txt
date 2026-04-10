cmake_minimum_required(VERSION 3.20)
project(blizorukost VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
find_package(LibtorrentRasterbar QUIET)

if(NOT LibtorrentRasterbar_FOUND)
    message(STATUS "[blizorukost] libtorrent not found via CMake; "
                   "trying pkg-config")
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(LIBTORRENT IMPORTED_TARGET libtorrent-rasterbar)
    endif()

    if(LIBTORRENT_FOUND)
        set(LT_TARGET PkgConfig::LIBTORRENT)
    else()
        message(STATUS "[blizorukost] Fetching libtorrent from GitHub (v2.0.10)")
        include(FetchContent)
        FetchContent_Declare(
            libtorrent
            GIT_REPOSITORY https://github.com/arvidn/libtorrent.git
            GIT_TAG        v2.0.10
            GIT_SHALLOW    TRUE
        )
        set(BUILD_SHARED_LIBS           OFF CACHE BOOL "" FORCE)
        set(deprecated-functions        OFF CACHE BOOL "" FORCE)
        set(CMAKE_POSITION_INDEPENDENT_CODE ON)
        FetchContent_MakeAvailable(libtorrent)
        set(LT_TARGET LibtorrentRasterbar::torrent-rasterbar)
    endif()
else()
    set(LT_TARGET LibtorrentRasterbar::torrent-rasterbar)
endif()

# ── blizorukost static library ───────────────────────────────────────────
add_library(blizorukost STATIC
    src/session.cpp
    src/api.cpp
)

target_include_directories(blizorukost
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(blizorukost PRIVATE ${LT_TARGET})

set_target_properties(blizorukost PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME blizorukost
)

# ── Platform link flags ───────────────────────────────────────────────────
if(APPLE)
    target_link_libraries(blizorukost PRIVATE
        "-framework SystemConfiguration"
        "-framework CoreFoundation"
        "-framework IOKit"
    )
elseif(UNIX)
    target_link_libraries(blizorukost PRIVATE pthread)
endif()

# ── Install ───────────────────────────────────────────────────────────────
install(TARGETS blizorukost ARCHIVE DESTINATION lib)
install(FILES include/blizorukost.h DESTINATION include)
