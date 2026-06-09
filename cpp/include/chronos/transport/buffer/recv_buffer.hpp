#pragma once

//receive buffer
//wraps the ring buffer and sits b/w the kernel socket and protocol layer

#include "ring_buffer.hpp"
#include "../../protocol/frame.hpp"
#include "../../protocol/frame_decoder.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <vector>
#include <span>

//posix socket includes
#include <sys/socket.h>
#include <sys/types.h>

namespace chronos::transport
{
    /**
    * @brief bridges the kernel tcp socket and protocol layer
    * manages a fixed capacity ring-buffer to reassemble wire streams into discrete frames
    */

    class RecvBuffer
    {
        public:

            //an explicit sentinel to separate backpressure full-state from orderly EOF (0)
            static constexpr ssize_t kBufferFull = -2;

            /**
            * @brief constructs a new receive buffer
            * @param capacity - total buffer size in bytes, defaults to 64KB
            */

            explicit RecvBuffer(size_t capacity = 65536) : ring_(capacity) {}

            //move only semantics
            RecvBuffer(const RecvBuffer &) = delete;
            RecvBuffer &operator=(const RecvBuffer &) = delete;
            RecvBuffer(RecvBuffer &&) noexcept = default;
            RecvBuffer &operator=(RecvBuffer &&) noexcept = default;

            /**
            * @brief returns the total number of unprocessed bytes in the buffer
            */

            [[nodiscard]] size_t buffered() const noexcept
            {
                return ring_.readable();
            }

            /**
            * @brief check if the buffer is in an unrecoverable/broken state
            */
            [[nodiscard]] bool isUnrecoverable() const noexcept
            {
                return is_unrecoverable_;
            }

            /**
            * @brief pulls raw bytes directly from the kernel to the ring buffer
            * zero-copy read into the contiguous writable segment of the ring buffer
            * @param fd - the socket file descriptor to read from
            * @return ssize_t - bytes received, 0 on shutdown, -1 on error
            */

            ssize_t fillFromSocket(int fd) noexcept
            {
                //if stream is poisoned, refuse to read more data from the kernel
                if (is_unrecoverable_) return -1;

                const size_t max_read = ring_.writableContiguous();

                //if the buffer is completely full, more cannot be read at the moment
                //app must extract frames to free up space
                if (max_read == 0) return kBufferFull;

                //zero-copy into buffer's backing memory
                ssize_t bytes_received = ::recv(fd, ring_.writableData(), max_read, 0);

                if (bytes_received > 0)
                    ring_.commit(static_cast<size_t>(bytes_received));

                return bytes_received;
            }

            /**
            * @brief attempt to extract a complete Frame from the buffered byte stream 
            * @return std::nullopt if complete frame hasn't arrived yet
            * @return std::expected<Frame, DecodeError> if enough bytes are present (success or failure)
            */

            [[nodiscard]] std::optional<std::expected<protocol::Frame, protocol::DecodeError>> tryExtractFrame()
            {

                if (is_unrecoverable_) return std::nullopt;

                //peek the header, get the total frame length
                std::array<std::byte, protocol::kHeaderSize> header_buf;
                if (!ring_.peek(header_buf.data(), protocol::kHeaderSize)) return std::nullopt;                

                uint32_t payload_length{};
                std::memcpy(&payload_length, header_buf.data() + 16, sizeof(uint32_t));

                if constexpr (std::endian::native == std::endian::little)
                    payload_length = std::byteswap(payload_length);

                const size_t total_frame_size = protocol::kHeaderSize + payload_length;

                //drop the connection if a single frame exceeds buffer capacity
                if (total_frame_size > ring_.capacity())
                {
                    poisonedStream();
                    return std::unexpected(protocol::DecodeError::PayloadLengthMismatch);
                }

                if (ring_.readable() < total_frame_size)
                    return std::nullopt;

                std::expected<protocol::Frame, protocol::DecodeError> decode_result;

                if (total_frame_size <= ring_.readableContiguous())
                {
                    //hot path
                    //create a zero-copy view directly pointing to the buffer's live memory
                    //decoder parses header in place and copies payload exactly once to returning
                    //Frame's vector
                    //then consume bytes from ring buffer once processing's finished
                    std::span<const std::byte> frame_view(ring_.readableData(), total_frame_size);
                    decode_result = protocol::FrameDecoder::decode(frame_view);
                }
                else
                {
                    //cold path
                    //extract full frame into contiguous buffer for the decoder
                    std::vector<std::byte> frame_data(total_frame_size); //frame straddles the circular wrap around boundary
                    if (!ring_.peek(frame_data.data(), total_frame_size)) return std::nullopt;
                    decode_result = protocol::FrameDecoder::decode(frame_data);
                }
                ring_.skip(total_frame_size);
                
                //any decoding failure means protocol desync
                //byte stream alignment is permanently lost, poison the stream immediately
                if (!decode_result.has_value())
                    poisonedStream();

                return decode_result;
            }

            [[nodiscard]] bool isFull() const noexcept
            {
                return ring_.writable() == 0;
            }

        private:

            void poisonedStream() noexcept
            {
                is_unrecoverable_ = true;
                ring_.skip(ring_.readable()); //drains remaining garbage to prevent stale re-reads
            }

            RingBuffer ring_;
            bool is_unrecoverable_{false};
    };
} //namespace chronos::transport