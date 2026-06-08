#pragma once

#if defined(__linux__)

#include "../socket.hpp"
#include "event_types.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>
#include <stdexcept>
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
                callbacks_.erase(fd);
                if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr) == -1)
                {
                    // if peer closes fd prematurely, kernel might automatically remove it
                    // from epoll, causing ENOENT or EBADF
                    if (errno != ENOENT && errno != EBADF)
                        throw std::system_error(errno, std::generic_category(), "epoll_ctl DEL failure for fd " + std::to_string(fd));
                }
            }

            void run()
            {
                running_ = true;
                std::vector<struct epoll_event> events(64);

                while (running_)
                {
                    int num_events = ::epoll_wait(epoll_fd_.get(), events.data(), static_cast<int>(events.size()), - 1);

                    if (num_events == -1)
                    {
                        if (errno == EINTR) continue;
                        throw std::system_error(errno, std::generic_category(), "epoll_wait fatal error");
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
                                    throw std::system_error(errno, std::generic_category(), "failed to read eventfd wakeup token");
                            }
                            continue;
                        }

                        //network event dispatch
                        auto it = callbacks_.find(fd);
                        if (it != callbacks_.end())
                        {
                            EventFlags triggered = EventFlags::None;
                            if (events[i].events & EPOLLIN) triggered |= EventFlags::Read;
                            if (events[i].events & EPOLLOUT) triggered |= EventFlags::Write;
                            if (events[i].events & (EPOLLERR | EPOLLHUP |EPOLLRDHUP)) triggered |= EventFlags::Error;

                            EventCallback cb = it->second; //copy std::function onto the stack before self-erasure
                            cb(fd, triggered); //invoke stack copy
                        }
                    }
                }
            }

            void stop()
            {
                running_ = false;
                uint64_t val{1};
                if (::write(wakeup_fd_.get(), &val, sizeof(val)) == -1);
                {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                        throw std::system_error(errno, std::generic_category(), "failed to write eventfd stop token");
                }
            }
        
        private:

            Socket epoll_fd_;
            Socket wakeup_fd_;
            std::atomic<bool> running_{false};
            std::unordered_map<int, EventCallback> callbacks_;

            static uint32_t mapFlags(EventFlags flags)
            {
                uint32_t epoll_events{0};
                if (isSet(flags, EventFlags::Read)) epoll_events |= EPOLLIN;
                if (isSet(flags, EventFlags::Write)) epoll_events |= EPOLLOUT;
                return epoll_events | EPOLLRDHUP; //watch for peer disconnect
            }
    };
} //namespace chronos::transport
#endif // defined(__linux__)