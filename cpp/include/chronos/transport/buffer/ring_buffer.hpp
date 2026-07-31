#pragma once

// basis: lmax disruptor

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>
#include <cassert>

namespace chronos::transport
{
    /**
    * @brief fixed capacity circular buffer
    * enforces power-of-2 sizing for bitmask-based indexing
    * ever-increasing read/write counters to track state
    */

    /// @brief default allocation for the transport buffers
    /// buffers are to grow on demand up to protocol::kMaxFrameSize
    /// message size limit is protocol::kMaxPayloadSize
    inline constexpr size_t kDefBufferCap{65536};

    class RingBuffer
    {
        public:

            /**
            @brief construct RingBuffer
            @param req_capacity rounded to the nearest power of 2
            */

            explicit RingBuffer(size_t req_capacity)
            {
                assert(req_capacity > 0 && "RingBuffer capacity must be non-zero");
                capacity_ = std::bit_ceil(req_capacity);
                mask_ = capacity_ - 1;
                buffer_.resize(capacity_);
            }

            RingBuffer(const RingBuffer &) = delete;
            RingBuffer &operator=(const RingBuffer &) = delete;
            RingBuffer(RingBuffer &&) = default;
            RingBuffer &operator=(RingBuffer &&) noexcept = default;

            /**
            * @brief bytes currently available to read
            */

            [[nodiscard]] size_t readable() const noexcept
            {
                return write_posn_ - read_posn_;
            }

            /**
            * @brief currently available free space
            */

            [[nodiscard]] size_t writable() const noexcept
            {
                return capacity_ - readable();
            }

            /**
            * @brief push bytes onto buffer
            * @return true if all bytes are written, false if space is insufficient
            */

            bool write(const std::byte *data, size_t len) noexcept
            {
                if (writable() < len) return false;

                const size_t offset = write_posn_ & mask_;
                const size_t first_chunk = std::min(len, capacity_ - offset);

                // write the first part up to the physical end of the vector
                std::memcpy(buffer_.data() + offset, data, first_chunk);

                if (first_chunk < len)
                    std::memcpy(buffer_.data(), data + first_chunk, len - first_chunk);

                write_posn_ += len;
                return true;
            }

            /**
            * @brief copy bytes out of the buffer without advancing the read pointer
            * @return true if all required bytes are peeked, false if insufficient readable bytes
            */

            [[nodiscard]] bool peek(std::byte *dest, size_t len) noexcept
            {
                if (readable() < len) return false;

                const size_t offset = read_posn_ & mask_;
                const size_t first_chunk = std::min(len, capacity_ - offset);

                std::memcpy(dest, buffer_.data() + offset, first_chunk);

                if (first_chunk < len) std::memcpy(dest + first_chunk, buffer_.data(), len - first_chunk);

                return true;
            }

            /**
            * @brief growth path for buffer on demand, buffer defaults to something small (64KiB)
            */

            void grow(size_t min_capacity)
            {
                if (min_capacity <= capacity_) return;

                size_t new_capacity{std::bit_ceil(min_capacity)};

                std::vector<std::byte> new_buffer(new_capacity);

                const size_t count = readable();

                if (count > 0)
                {
                    const size_t offset{read_posn_ & mask_};
                    const size_t first_chunk{std::min(count, capacity_ - offset)};

                    std::memcpy(new_buffer.data(), buffer_.data() + offset, first_chunk);

                    if (first_chunk < count) std::memcpy(new_buffer.data() + first_chunk, buffer_.data(), count - first_chunk);
                }

                buffer_ = std::move(new_buffer);

                capacity_ = new_capacity;
                mask_ = capacity_ - 1;
                read_posn_ = 0;
                write_posn_ = count;
            }

            /**
            * @brief copies bytes out of the buffer and advances the read pointer
            * @return true if successful, false if insufficient readbale bytes
            */

            bool read(std::byte *dest, size_t len) noexcept
            {
                if (peek(dest, len))
                {
                    read_posn_ += len;
                    return true;
                }

                return false;
            }

            /**
            * @brief advances read pointer without copying the data out
            * discards upto 'len' bytes
            */

            void skip(size_t len) noexcept
            {
                assert(len <= readable() && "read pointer advancement cannot exceed readable bytes");
                const size_t adv = std::min(len, readable());
                read_posn_ += adv;
            }

            /**
            * @brief return a raw pointer to the current read position
            * prses data directly out of the bufferr without copying
            */

            [[nodiscard]] const std::byte *readableData() const noexcept
            {
                return buffer_.data() + (read_posn_ & mask_);
            }

            /**
            * @brief return how many bytes can be read sequentially without wrapping
            * determines contiguity for readableData()
            */

            [[nodiscard]] size_t readableContiguous() const noexcept
            {
                const size_t offset = read_posn_ & mask_;
                return std::min(readable(), capacity_ - offset);
            }

            /**
            * @brief returns a raw pointer to the current write position
            * for receiving data from kernel sockets, say via recv()
            * caller must not write more than writeableContiguous() bytes
            */

            [[nodiscard]] std::byte *writableData() noexcept
            {
                return buffer_.data() + (write_posn_ & mask_);
            }

            /**
            * @brief returns how many bytes can be written sequentially without wrapping
            * necessary for zero-copy i/o
            */

            [[nodiscard]] size_t writableContiguous() const noexcept
            {
                const size_t offset = write_posn_ & mask_;
                return std::min(writable(), capacity_ - offset);
            }

            /**
            * @brief advances the write pointer after a direct memory write
            * tbu in conjunction with writableData()
            */

            void commit(size_t len) noexcept
            {
                assert(len <= writableContiguous() && "commit exceeds contiguous writable space");

                //clamping advancement for release build, prevents ring corruption
                const size_t adv = std::min(len, writableContiguous());
                write_posn_ += adv;
            }

            [[nodiscard]] size_t capacity() const noexcept
            {
                return capacity_;
            }

        private:

            std::vector<std::byte> buffer_;
            size_t capacity_{};
            size_t mask_{};

            //monotonically increasing counters
            size_t read_posn_{};
            size_t write_posn_{};
    };
} //namespace chronos::transport