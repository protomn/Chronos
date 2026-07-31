#pragma once

#include "protocol_version.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>
#include <array>

namespace chronos::protocol
{

    ///@brief exact header size when serialized to the wire
    inline constexpr size_t kHeaderSize{20};
    static_assert(kHeaderSize == 20, "Wire header contract must be exactly 20-bytes.");

    /// @brief: max total frame size on the wire
    inline constexpr size_t kMaxFrameSize{kMaxPayloadSize + kHeaderSize};

    ///@brief defines the administrative intent of the frame
    enum class FrameType : uint8_t
    {
        Request = 0x01,
        Response = 0x02,
        Error = 0x03,
        Ping = 0x04,
        Pong = 0x05,
        Cancel = 0x06,
        BeginStream = 0x07,
        StreamData = 0x08,
        EndStream = 0x09
    };

    ///@brief bitmask flags that modify frame processing behavior
    enum class FrameFlags : uint8_t 
    {
        None = 0x00,
        Compressed = 0x01,
        Streaming = 0x02,
        ProfilingOn = 0x04,
        ReplayOn = 0x08
    };

    /**
    * @brief bitwise operators for FrameFlags
    */

    [[nodiscard]] constexpr FrameFlags operator|(FrameFlags lhs, FrameFlags rhs) noexcept
    {
        return static_cast<FrameFlags>(static_cast<uint8_t>(lhs) | static_cast<uint8_t>(rhs));
    }

    [[nodiscard]] constexpr FrameFlags operator&(FrameFlags lhs, FrameFlags rhs) noexcept
    {
        return static_cast<FrameFlags>(static_cast<uint8_t>(lhs) & static_cast<uint8_t>(rhs));
    }

    [[nodiscard]] constexpr FrameFlags operator~(FrameFlags flags) noexcept
    {
        return static_cast<FrameFlags>(~static_cast<uint8_t>(flags));
    }

    inline constexpr FrameFlags &operator|=(FrameFlags &lhs, const FrameFlags &rhs) noexcept
    {
        lhs = lhs | rhs;
        return lhs;
    }

    inline constexpr FrameFlags &operator&=(FrameFlags &lhs, const FrameFlags &rhs) noexcept
    {
        lhs = lhs & rhs;
        return lhs;
    }

    ///@brief helper to check if any specific flag is set
    [[nodiscard]] constexpr bool isSet(FrameFlags flags, FrameFlags flag) noexcept
    {
        return (flags & flag) == flag;
    }

    /**
    * @brief data representation of the 20-byte wire header (frame metadata)
    * The in-memory size of struct may be larger than kHeaderSize (typically
    * 24 bytes due to alignment padding). Serializers write out fields seq-
    * uentially.
    */

    struct FrameHeader
    {
        std::array<uint8_t, 2> magic{kMagicBytes};
        uint8_t version{kCurrVersion};
        FrameType frame_type{FrameType::Request};
        FrameFlags frame_flags{FrameFlags::None};
        std::array<uint8_t, 3> reserved{0, 0, 0}; //pad to 8-byte boundary
        uint64_t request_id{0};
        uint32_t payload_length{0};
    };

    /**
    * @brief represents the complete protocol frame
    */
    
    struct Frame
    {
        FrameHeader header;
        std::vector<std::byte> payload;
    };

} //namespace chronos::protocol