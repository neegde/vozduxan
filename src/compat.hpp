/**
 * vozduxan — compat.hpp
 * Cross-platform socket abstraction (POSIX / Winsock2).
 */
#pragma once

#ifdef _WIN32
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  pragma comment(lib, "ws2_32.lib")

   /* socket handle type */
   using sock_t = SOCKET;
   static constexpr sock_t kInvalidSock = INVALID_SOCKET;

   inline void close_sock(sock_t s) { closesocket(s); }
   inline void shutdown_sock(sock_t s) { shutdown(s, SD_BOTH); }

   /* MSG_NOSIGNAL doesn't exist on Windows; send() never raises SIGPIPE */
#  ifndef MSG_NOSIGNAL
#    define MSG_NOSIGNAL 0
#  endif

   /* WSAPoll has the same signature as POSIX poll */
#  include <winsock2.h>
   inline int vozduxan_poll(struct pollfd* fds, unsigned long nfds, int timeout_ms) {
       return WSAPoll(fds, nfds, timeout_ms);
   }

   /* WSAStartup/Cleanup helpers */
   inline bool winsock_init() {
       WSADATA wsa;
       return WSAStartup(MAKEWORD(2, 2), &wsa) == 0;
   }
   inline void winsock_cleanup() { WSACleanup(); }

#else  /* POSIX */

#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <sys/socket.h>
#  include <unistd.h>
#  include <poll.h>

   using sock_t = int;
   static constexpr sock_t kInvalidSock = -1;

   inline void close_sock(sock_t s)    { close(s); }
   inline void shutdown_sock(sock_t s) { shutdown(s, SHUT_RDWR); }

   inline int vozduxan_poll(struct pollfd* fds, nfds_t nfds, int timeout_ms) {
       return poll(fds, nfds, timeout_ms);
   }

   inline bool winsock_init()    { return true; }
   inline void winsock_cleanup() {}

#endif
