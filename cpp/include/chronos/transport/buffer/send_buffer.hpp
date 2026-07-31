#pragma once

#include "chronos/protocol/frame.hpp"
#include "ring_buffer.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <span>

#include <sys/socket.h>
#include <sys/types.h>

// macOS/BSD compatibility: defines MSG_NOSIGNAL to 0 if it doesn't exist
// NOTE: on Apple Silicon, SO_NOSIGPIPE must be set via setsockopt()
// when the socket fiel descriptor is created to prevent SIGPIPE crashes

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace chronos::transport
{
    /**
    * @brief stages the encoded protocol bytes and flushes them to the kernel socket
    * operates on raw bytes only, no awareness of protocol level frame
    */

    class SendBuffer
    {
        public:

            /**
            * @brief constructor
            * @param initial_capacity - total buffer size in bytes, defaults to 64KB
            */
            explicit SendBuffer(size_t initial_capacity = kDefBufferCap) : ring_(initial_capacity) {}

            //move-only
            SendBuffer(const SendBuffer&) = delete;
            SendBuffer &operator=(const SendBuffer&) = delete;
            SendBuffer(SendBuffer &&) noexcept = default;
            SendBuffer &operator=(SendBuffer &&) noexcept = default;

            /**
            * @brief number of bytes currently staged and waiting to be sent
            */
            [[nodiscard]] size_t staged() const noexcept
            {
                return ring_.readable();
            }

            /**
            * @brief check if the buffer is completely drained
            */

            [[nodiscard]] bool empty() const noexcept
            {
                return staged() == 0;
            }

            /**
            * @brief stages encoded wire bytes into the buffer
            * @param data - non-owning view of raw-bytes to enqueue
            * @return true if all bytes were enqueued, false if insufficient space
            * @throws std::invalid_argument if Frame cannot fit within total buffer size
            */

            bool enqueue(std::span<const std::byte> data)
            {
                assert(data.size() <= protocol::kMaxFrameSize && "bounded by FrameEncoder");
                
                //if the frame is physically bigger than the entire ring, grow the ring to the required size
                if (data.size() > ring_.capacity())
                    ring_.grow(data.size());

                return ring_.write(data.data(), data.size());
            }

            /**
            * @brief flush stages bytes to kernel socket via zero-copy
            * @note since the underlying storage is a circular buffer, the data that wraps 
            * around the buffer boundary is non-contiguous. this method only flushes the first
            * contiguous chunk per call.
            * if wrap occurs under the level-triggered loop, the remaining tail will be picked up
            * and cleared on the immediate next event loop iteration since EPOLLOUT/EVFILT_WRITE
            * will remain asserted. if switching to an edge-triggered loop, this method must be 
            * altered to loop until either the buffer is completely empty, or hits EAGAIN.
            * @param fd - socket file descriptor
            * @return ssize_t - bytes successfully send, -1 on error
            */

            ssize_t flushToSocket(int fd) noexcept
            {
                const size_t max_write{ring_.readableContiguous()};

                //nothing to send/buffer empty
                if (max_write == 0) return 0;

                //zero-copy read from contiguous memory segment into the kernel
                ssize_t bytes_sent = ::send(fd, ring_.readableData(), max_write, MSG_NOSIGNAL);

                //advance read ptr if send() successful, even partially
                if (bytes_sent > 0) 
                    ring_.skip(static_cast<size_t>(bytes_sent));

                return bytes_sent;
            }

        private:

            RingBuffer ring_;
    };
} //namespace chronos::transport