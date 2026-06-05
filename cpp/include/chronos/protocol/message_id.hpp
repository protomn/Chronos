#pragma once

#include <atomic>
#include <cstdint>
#include <functional>

namespace chronos::protocol
{
    /**
    * @brief: A strong-typed 64-bit Message ID
    * Prevent accidental implicit conversions and mixing with raw ints.
    */

    struct MessageID
    {
        constexpr explicit MessageID(uint64_t id) noexcept : value_(id) {}

        constexpr MessageID() noexcept : value_(0) { }
        [[nodiscard]]constexpr uint64_t raw() const noexcept { return value_; }
        constexpr auto operator<=>(const MessageID&) const = default;

        /**
        * @brief: Automatically generates a globally unique MessageID
        */

        [[nodiscard]]static MessageID next() noexcept
        {
            // starts at 1, 0 can represent unintialized or invalid id
            static std::atomic<uint64_t> counter{1};
            return MessageID(counter.fetch_add(1, std::memory_order_relaxed));
        }

        private:
            uint64_t value_;
    };
} //namespace chronos::protocol

namespace std
{
    template<>
    struct hash<chronos::protocol::MessageID>
    {
        size_t operator()(const chronos::protocol::MessageID &id) const noexcept
        {
            return hash<uint64_t>{}(id.raw());
        }
    };
} // namespace std