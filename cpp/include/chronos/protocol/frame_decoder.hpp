#pragma once

#include "frame.hpp"

#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>
#include <cstring>

namespace chronos::protocol
{
    ///@brief failure modes encountered during wire decoding
    enum class DecodeError : uint8_t
    {
        BufferTooShort,
        InvalidMagic,
        IncompatibleVersion,
        UnknownFrameType,
        PayloadLengthMismatch,
        PayloadTooLarge
    };

    class FrameDecoder
    {
        public:

            //stateless utility enforcement
            FrameDecoder() = delete;

            /**
            * @brief decode a contiguous raw wire buffer into a valid frame object
            * @param buffer - a non-owning view of the received bytes
            * @return valid frame on success, DecodeError on failure
            */

            [[nodiscard]] static std::expected<Frame, DecodeError> decode(std::span<const std::byte> buffer)
            {
                if (buffer.size() < kHeaderSize)
                {
                    return std::unexpected(DecodeError::BufferTooShort);
                }

                const std::array<uint8_t, 2> decoded_magic{
                    static_cast<uint8_t>(buffer[0]),
                    static_cast<uint8_t>(buffer[1])
                };

                if (!validateMagicBytes(decoded_magic))
                {
                    return std::unexpected(DecodeError::InvalidMagic);
                }

                const auto decoded_version = static_cast<uint8_t>(buffer[2]);
                if (!isCompatible(decoded_version))
                {
                    return std::unexpected(DecodeError::IncompatibleVersion);
                }

                const auto raw_type = static_cast<uint8_t>(buffer[3]);
                if (raw_type < 0x01 || raw_type > 0x09)
                {
                    return std::unexpected(DecodeError::UnknownFrameType);
                }

                const auto decoded_type = static_cast<FrameType>(raw_type);

                const auto decoded_flags = static_cast<FrameFlags>(buffer[4]);

                const std::array<uint8_t, 3> decoded_reserve{
                    static_cast<uint8_t>(buffer[5]),
                    static_cast<uint8_t>(buffer[6]),
                    static_cast<uint8_t>(buffer[7])
                };

                const uint64_t decoded_request_id = readBigEndian<uint64_t>(buffer.data() + 8);
                const uint32_t decoded_payload_len = readBigEndian<uint32_t>(buffer.data() + 16);

                if (decoded_payload_len > kMaxPayloadSize)
                    return std::unexpected(DecodeError::PayloadTooLarge);

                if (buffer.size() != kHeaderSize + decoded_payload_len)
                    return std::unexpected(DecodeError::PayloadLengthMismatch);


                Frame frame;
                frame.header.magic = decoded_magic;
                frame.header.version = decoded_version;
                frame.header.frame_type = decoded_type;
                frame.header.frame_flags = decoded_flags;
                frame.header.reserved = decoded_reserve;
                frame.header.request_id = decoded_request_id;
                frame.header.payload_length = decoded_payload_len;

                //copy payload data of non-owning span (if exists)
                if (decoded_payload_len > 0)
                {
                    const auto *payload_start = buffer.data() + kHeaderSize;
                    frame.payload.assign(payload_start, payload_start + decoded_payload_len);
                }

                return frame;
            }

        private:

            /**
            * @brief read big-endian multi-byte int from raw memory and translate to native byte order
            */

            template<std::integral T>
            [[nodiscard]] static T readBigEndian(const std::byte *src) noexcept
            {
                T val{};
                std::memcpy(&val, src, sizeof(T));

                if constexpr (std::endian::native == std::endian::little)
                {
                    val = std::byteswap(val);
                }
                
                return val;
            }
    };

} //namespace chronos::protocol