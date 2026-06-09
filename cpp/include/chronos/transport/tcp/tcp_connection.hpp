#pragma once

#include "../socket.hpp"
#include "../buffer/recv_buffer.hpp"
#include "../buffer/send_buffer.hpp"
#include "../../protocol/frame.hpp"
#include "../../protocol/frame_encoder.hpp"

#include <cerrno>
#include <expected>
#include <optional>
#include <utility>
#include <vector>

#include <sys/socket.h>

namespace chronos::transport
{
    /**
    * @brief lifecycle state of a tcp connection
    */

    enum class ConnectionState : uint8_t
    {
        Connected,
        Closing,
        Closed,
        Error
    };

    /**
    * @brief manages a live tcp connection, binds a raw socket to zero-copy read/write buffers
    */

    class TcpConnection
    {
        public:

            /**
            * @brief construct new connection, take ownership of provided socket
            * @param sock - the connected socket fd wrapper
            */

            explicit TcpConnection(Socket sock) noexcept
                : socket_(std::move(sock)),
                  state_(ConnectionState::Connected) { }

            //move-only
            TcpConnection(const TcpConnection &) = delete;
            TcpConnection &operator=(const TcpConnection &) = delete;
            TcpConnection(TcpConnection &&) noexcept = default;
            TcpConnection &operator=(TcpConnection &&) noexcept = default;

            /**
            * @brief reads all available bytes from kernel to the receive buffer
            * @return bytes read, 0 if no data available (EAGAIN), -1 on fatal err
            */

            ssize_t receive() noexcept
            {
                if (state_ != ConnectionState::Connected) return -1;
                if (recv_buffer_.isFull()) return 0; //backpressure, not a disconnect

                ssize_t bytes_read = recv_buffer_.fillFromSocket(socket_.get());

                if (bytes_read == 0)
                {
                    //orderly peer EOF shutdown
                    state_ = ConnectionState::Closed;
                    return 0;
                }

                if (bytes_read == -1)
                {
                    //non-blocking socket has no data right now
                    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;

                    //real socket error occured
                    state_ = ConnectionState::Error;
                    return -1;
                }

                return bytes_read;
            }

            /**
            * @brief encodes a frame and stages its bytes in the send buffer
            * @param frame - the protocol-level frame to send
            * @return true if successfully staged, false if send buffer is full (backpressure)
            */

            [[nodiscard]] bool send(const protocol::Frame &frame)
            {
                if (state_ != ConnectionState::Connected) return false;

                std::vector<std::byte> encoded_bytes = protocol::FrameEncoder::encode(frame);
                //implicit conversion of std::vector to std::span for enqueue
                return send_buffer_.enqueue(encoded_bytes);
            }

            /**
            * @brief drains staged bytes from send buffer directly to the socket
            * @return bytes sent, 0 if socket is blocking (EAGAIN), -1 on fatal err
            */

            ssize_t flush() noexcept
            {
                if (state_ != ConnectionState::Connected && state_ != ConnectionState::Closing) return -1;
                
                ssize_t bytes_sent = send_buffer_.flushToSocket(socket_.get());

                if (bytes_sent == -1)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;

                    state_ = ConnectionState::Error;
                    return -1;
                }

                return bytes_sent;
            }

            /**
            * @brief attempt to extract a complete frame from buffered receive stream
            * post extraction buffer health eval, updates if fatal violation has occured
            * catches protocol desync err
            */

            [[nodiscard]] std::optional<std::expected<protocol::Frame, protocol::DecodeError>> tryExtractFrame()
            {
                auto res = recv_buffer_.tryExtractFrame();
                if (recv_buffer_.isUnrecoverable())
                    state_ = ConnectionState::Error;

                return res;
            }

            /**
            * @brief initiate a graceful shutdown of connection
            * signals remote peer no more data will be sent
            */

            void close() noexcept
            {
                if (state_ == ConnectionState::Connected)
                {
                    /*
                    * SHUT_WR send a TCP FIN packet, closing the write half of the connection
                    * underlying fd remains open until socket destructor runs
                    */
                    ::shutdown(socket_.get(), SHUT_WR);
                    state_ = ConnectionState::Closing;
                }
            }

            /**
            * @brief returns current state of connection
            */

            [[nodiscard]] ConnectionState state() const noexcept
            {
                return state_;
            }

            /**
            * @brief exposes raw fd for event loop reg
            */

            [[nodiscard]] int fd() const noexcept
            {
                return socket_.get();
            }

            /**
            * @brief checks for staged bytes waiting to be flushed
            */

            [[nodiscard]] bool hasPendingOutbound() const noexcept
            {
                return !send_buffer_.empty();
            }

        private:

            Socket socket_;
            RecvBuffer recv_buffer_;
            SendBuffer send_buffer_;
            ConnectionState state_;
    };
} //namespace chronos::transport