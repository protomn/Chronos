#pragma once

#include <array>
#include <cstdint>

namespace chronos::protocol
{
    /// @brief: magic bytes to identifies a valid chronos frame ('C', 'H')
    inline constexpr std::array<uint8_t, 2> kMagicBytes = {0x43, 0x48};

    /// @brief: current wire protocol version of this implementation
    inline constexpr uint8_t kCurrVersion{1};

    /// @brief: lowest wire protocol version this implemetation can safely understand
    inline constexpr uint8_t kMinCompatibleVersion{1};

    /**
    * @brief: Check is a remote protocol version is compatible with this implementation
    * A remote version is compatible if it is at least as high as the minimum compatible
    * version, and does not exceed the current version (unless forward compatibility is
    * supported, tbd later).
    * @param remote_version - version claimed by the remote peer
    * @return true if compatible, false if connection is to be rejected
    */

    [[nodiscard]] constexpr bool isCompatible(uint8_t remote_version) noexcept
    {
        return remote_version >= kMinCompatibleVersion && remote_version <= kCurrVersion;
    }

    /**
    * @brief: validate if given bytes match the expected magic bytes
    */

    [[nodiscard]] constexpr bool validateMagicBytes(std::array<uint8_t, 2> bytes) noexcept
    {
        return bytes == kMagicBytes;
    }
} //namespace chronos::protocol