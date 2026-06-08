#pragma once

#include <cstdint>
#include <functional>

namespace chronos::transport
{
    /**
    * @brief bitmask for event loop notifs
    * maps to EPOLLIN/EPOLLOUT on linux and EVFILT_READ/EVFILT_WRITE on macOS
    */

    enum class EventFlags : uint32_t
    {
        None = 0,
        Read = 1 << 0, // data available to read
        Write = 1 << 1, // buffer available to write
        Error = 1 << 2 // connection err/hangup
    };

    inline constexpr EventFlags operator|(EventFlags lhs, EventFlags rhs) noexcept
    {
        return static_cast<EventFlags>(static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs));
    }

    inline constexpr EventFlags operator&(EventFlags lhs, EventFlags rhs) noexcept
    {
        return static_cast<EventFlags>(static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs));
    }

    inline constexpr EventFlags &operator|=(EventFlags &lhs, const EventFlags &rhs) noexcept
    {
        lhs = static_cast<EventFlags>(static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs));
        return lhs;
    }

    inline constexpr EventFlags &operator&=(EventFlags &lhs, const EventFlags &rhs) noexcept
    {
        lhs = static_cast<EventFlags>(static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs));
        return lhs;
    }

    inline constexpr bool isSet(EventFlags flags, EventFlags flag) noexcept
    {
        return (flags & flag) == flag;
    }

    ///@brief callback signature fired when an fd has activity
    using EventCallback = std::function<void(int fd, EventFlags events)>;
} //namespace chronos::transport