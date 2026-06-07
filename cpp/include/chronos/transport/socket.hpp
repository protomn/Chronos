#pragma once

#include <unistd.h>
#include <utility>

namespace chronos::transport
{
    /**
    * @brief strict RAII wrapper around a POSIX fd
    * guarantees underlying socket is closed exactly once, when ownership ends
    */

    class Socket
    {
        public:

            /**
            * @brief take ownership of the raw fd
            * @param fd - the file descriptor to manage, default to -1 (invalid)
            */

            explicit Socket(int fd = -1) noexcept : fd_(fd) {}

            /**
            * @brief destroys the socket and closes fd if valid
            */

            ~Socket() { close(); }

            //no-copy to prevent double closes
            Socket(const Socket &) = delete;
            Socket &operator=(const Socket &) = delete;

            /**
            * @brief move-ctor to transfer ownership
            */

            Socket(Socket &&other) noexcept : fd_(other.release()) { }

            /**
            * @brief move-assgn closes current fd before taking ownership of a new one
            */
            Socket &operator=(Socket &&other) noexcept
            {
                if (this != &other)
                {
                    close();
                    fd_ = other.release();
                }

                return *this;
            }

            /**
            * @brief access the underlying raw file descriptor
            */

            [[nodiscard]] int get() const noexcept
            {
                return fd_;
            }

            /**
            * @brief checks if the wrapper currently holds a valid fd
            */

            [[nodiscard]] bool isValid() const noexcept
            {
                return fd_ != -1;
            }

            /**
            * @brief relinquish ownership of fd w/o closing it
            * @return raw fd, caller assumes responsibility for closing it
            */

            [[nodiscard]] int release() noexcept
            {   
                int temp{fd_};
                fd_ = -1;
                return temp;
            }

        private:

            int fd_{-1};

            /**
            @brief internal helper to close and invalidate the fd
            */

            void close() noexcept
            {
                if (fd_ != -1)
                {
                    ::close(fd_);
                    fd_ = -1;
                }
            }
    };
} //namespace chronos::transport