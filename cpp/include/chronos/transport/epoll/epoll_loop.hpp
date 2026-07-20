#pragma once

#if defined(__linux__)

#include "../socket.hpp"
#include "event_types.hpp"

#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <unordered_map>
#include <vector>
#include <stdexcept>
#include <string>
#include <system_error>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace chronos::transport
{
    class EpollLoop
    {
        public:

            EpollLoop()
                : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
                  wakeup_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC))
            {
                if (!epoll_fd_.isValid())
                    throw std::system_error(errno, std::generic_category(), "failed to create epoll instance.");
                if (!wakeup_fd_.isValid())
                    throw std::system_error(errno, std::generic_category(), "failed to create eventfd instance.");

                //register eventfd and wake up the loop
                struct epoll_event ev{};
                ev.events = EPOLLIN;
                ev.data.fd = wakeup_fd_.get();

                if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, wakeup_fd_.get(), &ev) == -1)
                    throw std::system_error(errno, std::generic_category(), "failed to register eventfd with epoll.");
            }

            void add(int fd, EventFlags events, EventCallback cb)
            {
                if (fd < 0) throw std::invalid_argument("invalid file descriptor");

                //flat-vector resize on demand to accomodate fd index
                if (static_cast<size_t>(fd) >= callbacks_.size())
                    callbacks_.resize(fd + 1);
                
                if (callbacks_[fd])
                    throw std::system_error(EEXIST, std::generic_category(), "epoll add failure: fd " + std::to_string(fd) + " already exists");

                struct epoll_event ev{};
                ev.events = mapFlags(events);
                ev.data.fd = fd;
                if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev) == -1)
                    throw std::system_error(errno, std::generic_category(), "epoll_ctl add failure for fd " + std::to_string(fd));

                callbacks_[fd] = std::move(cb);
            }

            void modify(int fd, EventFlags events)
            {
                struct epoll_event ev{};
                ev.events = mapFlags(events);
                ev.data.fd = fd;
                if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, fd, &ev) == -1)
                    throw std::system_error(errno, std::generic_category(), "epoll_ctl mod failure for fd " + std::to_string(fd));
            }

            void remove(int fd)
            {
                if (fd >= 0 && static_cast<size_t>(fd) < callbacks_.size())
                    callbacks_[fd] = nullptr;

                if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr) == -1)
                {
                    // if peer closes fd prematurely, kernel might automatically remove it
                    // from epoll, causing ENOENT or EBADF
                    if (errno != ENOENT && errno != EBADF)
                        throw std::system_error(errno, std::generic_category(), "epoll_ctl DEL failure for fd " + std::to_string(fd));
                }
            }

            /**
            * @brief background event loop driver
            * @return std::expected containing void on clean exit, error_code on failure
            */

            [[nodiscard]] std::expected<void, std::error_code> run() noexcept
            {
                running_ = true;
                std::vector<struct epoll_event> events(64);

                while (running_)
                {
                    int num_events = ::epoll_wait(epoll_fd_.get(), events.data(), static_cast<int>(events.size()), - 1);

                    if (num_events == -1)
                    {
                        if (errno == EINTR) continue;
                        return std::unexpected(std::error_code(errno, std::generic_category()));
                    }

                    for(auto i{0}; i < num_events; ++i)
                    {
                        int fd = events[i].data.fd;

                        if (fd == wakeup_fd_.get())
                        {
                            uint64_t val{};
                            if (::read(wakeup_fd_.get(), &val, sizeof(val)) == -1)
                            {
                                if (errno != EAGAIN && errno != EWOULDBLOCK)
                                    return std::unexpected(std::error_code(errno, std::generic_category()));
                            }
                            continue;
                        }

                        if (fd >= 0 && static_cast<size_t>(fd) < callbacks_.size() && callbacks_[fd])
                        {
                            EventFlags triggered = EventFlags::None;
                            if (events[i].events & EPOLLIN) triggered |= EventFlags::Read;
                            if (events[i].events & EPOLLOUT) triggered |= EventFlags::Write;
                            if (events[i].events & (EPOLLERR | EPOLLHUP |EPOLLRDHUP)) triggered |= EventFlags::Error;

                            EventCallback cb = callbacks_[fd]; //stack copy survives remove()/resize()

                            try
                            {
                                cb(fd, triggered); //invoke stack copy
                            }
                            catch (const std::exception &e)
                            {
                                //route to background log buffer in prod, tbd later
                                (void)e;
                            }
                            catch(...)
                            {
                                // increment telemetry counter 
                                // log anonymous warning to log subsystem, tbd later in prod build
                            }
                        }
                    }
                }
                return {};
            }

            void stop()
            {
                running_ = false;
                uint64_t val{1};
                if (::write(wakeup_fd_.get(), &val, sizeof(val)) == -1)
                {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                        throw std::system_error(errno, std::generic_category(), "failed to write eventfd stop token");
                }
            }
        
        private:

            Socket epoll_fd_;
            Socket wakeup_fd_;
            std::atomic<bool> running_{false};
            std::vector<EventCallback> callbacks_;

            static uint32_t mapFlags(EventFlags flags)
            {
                uint32_t epoll_events{0};
                if (isSet(flags, EventFlags::Read)) epoll_events |= EPOLLIN;
                if (isSet(flags, EventFlags::Write)) epoll_events |= EPOLLOUT;
                return epoll_events | EPOLLRDHUP; // watch for peer disconnect
            }
    };
} //namespace chronos::transport
#endif // defined(__linux__)