#pragma once

#include "ring_buffer.hpp"

#include <cstddef>
#include <cstdint>
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
            * @param capacity - total buffer size in bytes, defaults to 64KB
            */
            explicit SendBuffer(size_t capacity = 65536) : ring_(capacity) {}

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
            * @brief stages encoded wire bytes into the bufferf
            * @param data - non-owning view of raw-bytes to enqueue
            * @return true if all bytes were enqueued, false if insufficient space
            */

            bool enqueue(std::span<std::byte> data) noexcept
            {
                return ring_.write(data.data(), data.size());
            }

            /**
            * @brief flush stages bytes to kernel socket via zero-copy
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