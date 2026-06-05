#pragma once

#include "frame.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <concepts>
#include <vector>

namespace chronos::protocol
{
    class FrameEncoder
    {
        public:
            //pure stateless utility (for now)
            FrameEncoder() = delete;

            /**
            * @brief serialize a high-level Frame object into a sequence of big-endian wire bytes
            * @param frame - the native frame to encode
            * @return contiguous buffer containing the 20-byte buffer header and payload
            */
            
            [[nodiscard]] static std::vector<std::byte> encode(const Frame &frame)
            {
                std::vector<std::byte> buffer;

                const size_t total_size = kHeaderSize + frame.payload.size();
                buffer.reserve(total_size);

                //serialize magic bytes
                appendRaw(buffer, frame.header.magic.data(), frame.header.magic.size());
                
                //serialize version
                appendByte(buffer, frame.header.version);

                //serialize type
                appendByte(buffer, static_cast<uint8_t>(frame.header.frame_type));

                //serialize flags
                appendByte(buffer, static_cast<uint8_t>(frame.header.frame_flags));

                //serialize reserved bytes
                appendRaw(buffer, frame.header.reserved.data(), frame.header.reserved.size());

                //serialize request_id (big-endian)
                appendBigEndian(buffer, frame.header.request_id);

                //serialize payload
                // uses size of payload vector to maintain wire integrity
                const auto payload_len = static_cast<uint32_t>(frame.payload.size());
                appendBigEndian(buffer, payload_len);


                buffer.insert(buffer.end(), frame.payload.begin(), frame.payload.end()); //no-ops if buffer empty

                return buffer;
            }

        private:
            /**
            * @brief convert int value to big-endian and append
            */

            template<std::integral T>
            static void appendBigEndian(std::vector<std::byte> &buffer, T val)
            {
                if constexpr (std::endian::native == std::endian::little)
                {   
                    val = std::byteswap(val);
                }

                //treat val as a sequence of raw bytes
                const auto *byte_ptr = reinterpret_cast<const std::byte*>(&val);
                appendRaw(buffer, byte_ptr, sizeof(T));
            }

            /**
            * @brief append a single byte
            */
            
            static void appendByte(std::vector<std::byte> &buffer, uint8_t val)
            {
                buffer.push_back(static_cast<std::byte>(val));
            }

            /**
            * @brief append raw contiguous bytes directly into vector
            */

            static void appendRaw(std::vector<std::byte> &buffer, const void *src, size_t size)
            {
                const auto *byte_ptr = static_cast<const std::byte*>(src);
                buffer.insert(buffer.end(), byte_ptr, byte_ptr + size);
            }
    };
} //namespace chronos::protocol