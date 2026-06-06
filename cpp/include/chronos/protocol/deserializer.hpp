#pragma once

#include "frame.hpp"
#include "request.hpp"
#include "response.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::protocol
{   
    /**
    * @brief failure modes that may be encountered during deserialization
    */

    enum class DeserializerError : uint8_t
    {
        WrongFrameType,
        PayloadTooShort,
        InvalidMethodName,
        InvalidStatusCode
    };

    /**
    * @brief translate wire-ready frame objects back into app-level
    * for Request and Response objects
    */

    class Deserializer
    {
        public:

            Deserializer() = delete;

            /**
            * @brief reconstruct a request from an inbound frame.
            * expected format: [2 bytes: method name len][N bytes: method name][remaining: payload]
            */

            [[nodiscard]] static std::expected<Request, DeserializerError> deserializeRequest(Frame &&frame)
            {
                if (frame.header.frame_type != FrameType::Request)
                    return std::unexpected(DeserializerError::WrongFrameType);

                size_t offset{};

                // read method name len
                auto method_len_opt = readU16BigEndian(frame.payload, offset);
                if(!method_len_opt) return std::unexpected(DeserializerError::PayloadTooShort);
                uint16_t method_len = *method_len_opt;

                if (frame.payload.size() - offset < method_len) return std::unexpected(DeserializerError::PayloadTooShort);
                if (method_len == 0) return std::unexpected(DeserializerError::InvalidMethodName);

                // get method name
                std::string method_name(
                    reinterpret_cast<const char*>(frame.payload.data() + offset),
                    method_len
                );

                offset += method_len;

                // deserialize remaining payload
                std::vector<std::byte> payload{};
                if (offset < frame.payload.size())
                    payload.assign(
                        frame.payload.begin() + offset,
                        frame.payload.end()
                    );

                return Request::reconstruct(MessageID(frame.header.request_id), 
                                        std::move(method_name), 
                                        std::move(payload),
                                        frame.header.frame_flags);
            }

            /**
            * @brief reconstruct a response from an inbound frame
            * expected format: [1 byte: status code][2 bytes: error msg len][N bytes: error msg][remaining: payload]
            */

            [[nodiscard]] static std::expected<Response, DeserializerError> deserializeResponse(Frame &&frame)
            {
                if (frame.header.frame_type != FrameType::Response
                    && frame.header.frame_type != FrameType::Error) return std::unexpected(DeserializerError::WrongFrameType);

                size_t offset{};

                auto status_opt = readU8(frame.payload, offset);
                if (!status_opt) return std::unexpected(DeserializerError::InvalidStatusCode);
                uint8_t raw_status = *status_opt;
                if (raw_status > kMaxStatusCode) return std::unexpected(DeserializerError::InvalidStatusCode);
                StatusCode status = static_cast<StatusCode>(raw_status);

                auto err_len_opt = readU16BigEndian(frame.payload, offset);
                if (!err_len_opt) return std::unexpected(DeserializerError::PayloadTooShort);
                uint16_t err_len = *err_len_opt;
                if (frame.payload.size() - offset < err_len) return std::unexpected(DeserializerError::PayloadTooShort);

                std::optional<std::string> err_msg{std::nullopt};
                if (err_len > 0)
                {
                    err_msg = std::string(
                        reinterpret_cast<const char *>(frame.payload.data() + offset),
                        err_len
                    );

                    offset += err_len;
                }

                std::vector<std::byte> payload{};
                if (offset < frame.payload.size())
                    payload.assign(
                        frame.payload.begin() + offset,
                        frame.payload.end()
                    );

                return Response::reconstruct(
                    MessageID(frame.header.request_id),
                    status,
                    std::move(payload),
                    std::move(err_msg),
                    frame.header.frame_flags
                );
            }

        private:
            
            /**
            @brief read a 16-bit int in big-endian format and advance offset
            */

            static std::optional<uint16_t> readU16BigEndian(const std::vector<std::byte> &buffer, size_t &offset)
            {
                if (offset + sizeof(uint16_t) > buffer.size()) return std::nullopt; 

                uint16_t val{};
                std::memcpy(&val, buffer.data() + offset, sizeof(uint16_t));

                if constexpr (std::endian::native == std::endian::little)
                    val = std::byteswap(val);

                offset += sizeof(uint16_t);
                return val;
            }

            /**
            * @brief read an 8-bit unsigned int and advance offset
            */

            static std::optional<uint8_t> readU8(const std::vector<std::byte> &buffer, size_t &offset)
            {
                if (offset + sizeof(uint8_t) > buffer.size()) return std::nullopt;

                uint8_t val{static_cast<uint8_t>(buffer[offset])};
                offset += sizeof(uint8_t);
                return val;
            }
    };
} //namespace chronos::protocol
