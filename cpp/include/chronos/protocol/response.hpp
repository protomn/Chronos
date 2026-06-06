#pragma once

#include "message.hpp"
#include "message_id.hpp"

#include <cstdint>
#include <cstddef>
#include <optional>
#include <string>
#include <vector>
#include <utility>

namespace chronos::protocol
{
    /**
    * @brief app-level rpc execution status
    */

    enum class StatusCode : uint8_t
    {
        Ok = 0,
        InternalError = 1,
        HandlerNotFound = 2,
        Timeout = 3,
        Cancelled = 4,
        InvalidRequest = 5
    };

    ///@brief highest valid underlying value for status code, to be updated when adding more status codes
    inline constexpr uint8_t kMaxStatusCode{static_cast<uint8_t>(StatusCode::InvalidRequest)};

    /**
    * @brief representation of outbound or inbound rpc response
    */

    class Response
    {
        public:

            ///@brief underlying message
            Message message;

            ///@brief error description, to be populated only on failure
            std::optional<std::string> error_msg;

            ///@brief execution status of rpc call
            StatusCode status;

            /**
            * @brief static factory to create new response
            * derives MessageType based on provided StatusCode
            * @param req_id id of the request this response corresponds to
            * @param status_code execution result of rpc handler
            * @param payload response data (default: empty)
            * @param err_msg error message on failure (default: empty)
            * @param flags processing flags for the wire
            */

            [[nodiscard]] static Response create(
                MessageID req_id,
                StatusCode status_code,
                std::vector<std::byte> payload = {},
                std::optional<std::string> err_msg = std::nullopt,
                FrameFlags flags = FrameFlags::None)
            {
                // mapping status to the protocol-level message type
                MessageType type = (status_code == StatusCode::Ok)
                                    ? MessageType::Response
                                    : MessageType::Error;

                Message msg(
                    type, 
                    req_id,
                    flags,
                    std::move(payload)
                );

                return Response(std::move(msg), std::move(err_msg), status_code);
            }

            /**
            @brief static factory for reconstructing inbound Response from wire data
            * uses existing MessageID
            */

            [[nodiscard]] static Response reconstruct(
                MessageID id,
                StatusCode code,
                std::vector<std::byte> payload = {},
                std::optional<std::string> err_msg = std::nullopt,
                FrameFlags flags = FrameFlags::None)
            {
                MessageType type = (code == StatusCode::Ok)
                                    ? MessageType::Response
                                    : MessageType::Error;
                
                Message msg(type, id, flags, std::move(payload));
                return Response(std::move(msg), std::move(err_msg), code);
            }

            /**
            * @brief helper to check if rpc call has succeeded
            */

            [[nodiscard]] bool hasSucceeded() const noexcept
            {
                return status == StatusCode::Ok;
            }

        private:

            /**
            * @brief private ctor to force instantiation via create()
            */

            Response(
                Message msg,
                std::optional<std::string> err_msg,
                StatusCode status_code)
                : message(std::move(msg)),
                  error_msg(std::move(err_msg)),
                  status(status_code)
            {}
    };
} //namespace chronos::protocol