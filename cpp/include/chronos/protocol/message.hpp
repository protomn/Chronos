#pragma once

#include "frame.hpp"
#include "message_id.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace chronos::protocol
{
    ///@brief app-level intent of message, decoupled from wire transport
    enum class MessageType : uint8_t
    {
        Request = 1,
        Response = 2,
        Error = 3
    };

    /**
    * @brief intermediate representation for mapping wire frames to app logic
    * foundation for request and response objects
    */

    struct Message
    {
        /**
        * message members are ordered from largest to smallest alignment to allow the
        * compiler to tightly pack it and avoid alignment gaps
        * Message is now 48-bytes
        */
        std::vector<std::byte> payload; //24-bytes, alignment 8

        ///@brief profiler hook: record the exact moment when message was instantiated
        std::chrono::steady_clock::time_point instantiated_at; //8-bytes, alignment 8

        MessageID id; //8-bytes, alignment 8
        FrameFlags flags; //1-byte, alignment 1
        MessageType type; //1-byte, 6-bytes padding, alignment 1
    
        /**
        * @brief construct a new message
        * instantiated_at is automatically captured upon instantiation
        */

        Message(MessageType msg_type, MessageID msg_id, FrameFlags msg_flags, std::vector<std::byte> msg_payload = {})
            : payload(std::move(msg_payload)),
              instantiated_at(std::chrono::steady_clock::now()),
              id(msg_id),
              flags(msg_flags),
              type(msg_type)
        {}

        /**
        * @brief Message is a move-only type. 
        * Memory ownership is always to be explicitly transferred via std::move() to
        * prevent silent deep copies if a Message object gets accidentally passed by
        * value
        */

        Message(const Message&) = delete;
        Message& operator=(const Message&) = delete;

        Message(Message&&) = default;
        Message& operator=(Message&&) = default;
    };

    ///Safety check - guard against future change accidentally inflating Message object size
    static_assert(sizeof(Message) == 48, "Message layout changed, recheck member ordering.");
} //namespace chronos::protocol