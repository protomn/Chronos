#pragma once

#include "frame.hpp"
#include "request.hpp"
#include "response.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <vector>
#include <cassert>
#include <limits>

namespace chronos::protocol
{
    /**
    * @brief serialization failure conditions
    */

    enum class SerializerError : uint8_t
    {
        MethodNameTooLong,
        ErrorMessageTooLong
    };

    /**
    * @brief translate request and response objects into wire-ready
    * Frame objects using length-prefixed binary layout
    */

    class Serializer
    {
        public:

            // stateless utility class
            Serializer() = delete;

            /**
            * @brief serialize outbound rpc request into a Frame object
            * format: [2 bytes: method name len][N bytes: method name][remaining: payload]
            */

            [[nodiscard]] static std::expected<Frame, SerializerError> serialize(const Request &req)
            {
                //runtime check
                if (req.method.size() > std::numeric_limits<uint16_t>::max())
                    return std::unexpected(SerializerError::MethodNameTooLong);

                const uint16_t method_len = static_cast<uint16_t>(req.method.size());

                Frame frame;
                frame.header.frame_type = FrameType::Request;
                frame.header.frame_flags = req.message.flags;
                frame.header.request_id = req.message.id.raw();

                assert(req.method.size() <= std::numeric_limits<uint16_t>::max()
                       && "method name exceeds maximum encodable length");

                // buffer pre-allocation to exact size
                std::vector<std::byte> payload;
                payload.reserve(sizeof(uint16_t) + method_len + req.message.payload.size());

                //encoding message len
                writeU16BigEndian(payload, method_len);

                //encoding method name
                writeRaw(payload, req.method.data(), method_len);

                //encode payload
                writeRaw(payload, req.message.payload.data(), req.message.payload.size());

                frame.payload = std::move(payload);
                frame.header.payload_length = static_cast<uint32_t>(frame.payload.size());

                return frame;
            }

            /**
            * @brief serialize outbound rpc response into a frame object
            * format: [1 byte: status code][2 bytes: error msg len][N bytes: error msg][remaining: payload]
            */

            [[nodiscard]] static std::expected<Frame, SerializerError> serialize(const Response &resp)
            {
                //runtime check
                if(resp.error_msg.has_value() && (resp.error_msg->size() > std::numeric_limits<uint16_t>::max()))
                    return std::unexpected(SerializerError::ErrorMessageTooLong);

                const uint16_t err_len = resp.error_msg.has_value()
                                         ? static_cast<uint16_t>(resp.error_msg->size())
                                         : 0;

                Frame frame;
                frame.header.frame_type = resp.hasSucceeded() ? FrameType::Response : FrameType::Error;
                frame.header.frame_flags = resp.message.flags;
                frame.header.request_id = resp.message.id.raw();

                // buffer pre-allocation to exact size
                std::vector<std::byte> payload;
                payload.reserve(sizeof(uint8_t) + sizeof(uint16_t) + err_len + resp.message.payload.size());

                //encoding status code
                writeU8(payload, static_cast<uint8_t>(resp.status));

                //encoding err_msg len
                writeU16BigEndian(payload, err_len);

                //encode err message (if present)
                if (err_len > 0)
                {
                    writeRaw(payload, resp.error_msg->data(), err_len);
                }

                //encoding payload
                writeRaw(payload, resp.message.payload.data(), resp.message.payload.size());

                frame.payload = std::move(payload);
                frame.header.payload_length = static_cast<uint32_t>(frame.payload.size());

                return frame;
            }

        private:

            /**
            * @brief write 16-bit unsigned int to buffer in network byte order
            */

            static void writeU16BigEndian(std::vector<std::byte> &buffer, uint16_t val)
            {
                if constexpr (std::endian::native == std::endian::little)
                {
                    val = std::byteswap(val);
                }

                const auto *byte_ptr = reinterpret_cast<const std::byte*>(&val);
                writeRaw(buffer, byte_ptr, sizeof(uint16_t));
            }

            /**
            * @brief write single 8-bit unsigned int byte
            */

            static void writeU8(std::vector<std::byte> &buffer, uint8_t val)
            {
                buffer.push_back(static_cast<std::byte>(val));
            }

            /**
            * @brief write raw contiguous bytes straight to buffer
            */

            static void writeRaw(std::vector<std::byte> &buffer, const void *src, size_t size)
            {
                if (size == 0) return;

                const auto *byte_ptr = static_cast<const std::byte*>(src);
                buffer.insert(buffer.end(), byte_ptr, byte_ptr + size);
            }
    };
} //namespace chronos::protocol