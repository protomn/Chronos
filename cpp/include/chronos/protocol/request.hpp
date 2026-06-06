#pragma once

#include "message.hpp"

#include <chrono>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <cstddef>

namespace chronos::protocol
{
    /**
    * @brief app-level representation of inbound or outbound rpc request
    * move-only semantics via underlying message member
    */

    class Request
    {
        public:

            ///@brief message member containing the payload, id, flags and type
            Message message;

            ///@brief target rpc handler/function name to where the request is to be routed
            std::string method;

            ///@brief optional absolute deadline for when the request becomes invalid
            std::optional<std::chrono::steady_clock::time_point> deadline;

            /**
            * @brief static factory to create new request
            * automatically generate unique message_id and set message_type
            */

            [[nodiscard]] static Request create(
                std::string method,
                std::vector<std::byte> payload,
                FrameFlags flags = FrameFlags::None,
                std::optional<std::chrono::steady_clock::time_point> request_deadline = std::nullopt
            )
            {
                Message msg(
                    MessageType::Request,
                    MessageID::next(),
                    flags,
                    std::move(payload)
                );

                return Request(std::move(msg), std::move(method), std::move(request_deadline));
            }

            /**
            * @brief static factory to reconstruct inbound request from wire data
            * uses existing MessageID rather than generating a new one
            */

            [[nodiscard]] static Request reconstruct(
                MessageID id,
                std::string method,
                std::vector<std::byte> payload,
                FrameFlags flags = FrameFlags::None,
                std::optional<std::chrono::steady_clock::time_point> req_deadline = std::nullopt)
            {
                Message msg(MessageType::Request, id, flags, std::move(payload));
                return Request(std::move(msg), std::move(method), std::move(req_deadline));
            }

            /**
            * @brief check if the request has passed the deadline
            * @return true if set deadline has elapsed, false otherwise
            */

            [[nodiscard]] bool pastDeadline() const noexcept
            {
                if(deadline.has_value())
                {
                    return std::chrono::steady_clock::now() >= *deadline;
                }

                return false;
            }

        private:

            /**
            * @brief private constructor to force instantiation via create() factory
            */

            Request(
                Message msg_,
                std::string method_,
                std::optional<std::chrono::steady_clock::time_point> request_deadline_)
                    : message(std::move(msg_)),
                      method(std::move(method_)),
                      deadline(std::move(request_deadline_))
            {}
    };
} //namespace chronos::protocol