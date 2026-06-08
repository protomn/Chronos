#pragma once

#include "event_types.hpp"

// compile-time target selection

#if defined(__linux__)
    #include "epoll_loop.hpp"
    namespace chronos::transport
    {
        using EventLoop = EpollLoop;
    }
#elif defined(__APPLE__) | defined(__FreeBSD__)
    #include "kqueue_loop.hpp"
    namespace chronos::transport
    {
        using EventLoop = KqueueLoop;
    }
#else
    #error "unsupported OS, chronos requires epoll (linux) or kqueue (macOS/BSD)."
#endif