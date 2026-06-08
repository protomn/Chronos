#pragma once

#include <string>
#if defined(__APPLE__) | defined(__FreeBSD__)

#include "../socket.hpp"
#include "event_types.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>
#include <stdexcept>
#include <system_error>

//macOS / BSD specific headers
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

namespace chronos::transport
{
    /**
    * @brief the transport layer
    * drives async i/o via native mac kqueue system
    */

    class KqueueLoop
    {
        public:

            KqueueLoop() : kq_(::kqueue())
            {
                if (!kq_.isValid()) throw std::system_error(errno, std::generic_category(), "failed to create kqueue instance");

                //kqueue user-event to act as cross-thread wakeup signal
                struct kevent wake_event{};
                EV_SET(&wake_event, 0, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);

                if (::kevent(kq_.get(), &wake_event, 1, nullptr, 0, nullptr) == -1)
                    throw std::system_error(errno, std::generic_category(), "failed to register kqueue user event.");
            }

            /**
            * @brief register a new fd with the event loop
            */

            void add(int fd, EventFlags events, EventCallback cb)
            {
                // maintains transactional safety by applying mod to kernel first
                //if modify() throws due to a bad fd, callback is never committed to the map
                modify(fd, events);
                callbacks_[fd] = std::move(cb);
            }

            /**
            * @brief change watched events for an existing fd
            */

            void modify(int fd, EventFlags events)
            {
                std::vector<struct kevent> changes;
                changes.reserve(2);
                
                if (isSet(events, EventFlags::Read))
                {
                    struct kevent ev{};
                    EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
                    changes.push_back(ev);
                }
                else
                {
                    struct kevent ev{};
                    EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_DISABLE, 0, 0, nullptr);
                    changes.push_back(ev);
                }

                if (isSet(events, EventFlags::Write))
                {
                    struct kevent ev{};
                    EV_SET(&ev, fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, nullptr);
                    changes.push_back(ev);
                }
                else
                {
                    struct kevent ev{};
                    EV_SET(&ev, fd, EVFILT_WRITE, EV_ADD | EV_DISABLE, 0, 0, nullptr);
                    changes.push_back(ev);
                }

                if (::kevent(kq_.get(), changes.data(), static_cast<int>(changes.size()), nullptr, 0, nullptr) == -1)
                {
                    // when deleting a filter that wasn't previously active, kqueue may return ENOENT
                    //suppresses that specific error, lets other real errors like EBADF pass through
                    if (errno != ENOENT)
                        throw std::system_error(errno, std::generic_category(), "kevent modify failed for fd " + std::to_string(fd));
                }
            }

            /**
            * @brief reves an fd from the event loop
            */

            void remove(int fd)
            {
                callbacks_.erase(fd);

                struct kevent changes[2];
                EV_SET(&changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
                EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);

                if (::kevent(kq_.get(), changes, 2, nullptr, 0, nullptr) == -1)
                {
                    if (errno != ENOENT && errno != EBADF)
                        throw std::system_error(errno, std::generic_category(), "kevent remove failed for fd " + std::to_string(fd));
                }
            }

            /**
            * @brief blocks current thread waiting for dispatching i/o events
            */

            void run()
            {
                running_ = true;
                std::vector<struct kevent> events(64);

                while (running_)
                {
                    int num_events = ::kevent(kq_.get(), nullptr, 0, events.data(), static_cast<int>(events.size()), nullptr);

                    if (num_events == -1)
                    {
                        if (errno == EINTR) continue;
                        throw std::system_error(errno, std::generic_category(), "kevent wait fatal error");
                    }

                    for (auto i{0}; i < num_events; ++i)
                    {
                        const auto &ev = events[i];

                        if (ev.filter == EVFILT_USER) continue; //thread wakeup signal (stop() was called)

                        int fd = static_cast<int>(ev.ident);
                        auto it = callbacks_.find(fd);

                        if (it != callbacks_.end())
                        {
                            EventFlags triggered_flags{EventFlags::None};

                            if (ev.filter == EVFILT_READ) triggered_flags |= EventFlags::Read;
                            if (ev.filter == EVFILT_WRITE) triggered_flags |= EventFlags::Write;
                            if (ev.flags & (EV_EOF | EV_ERROR)) triggered_flags |= EventFlags::Error;

                            EventCallback cb = it->second; //copy std::function onto the stack before self-erasure
                            cb(fd, triggered_flags); //invoke safe stack copy
                        }
                    }
                }
            }

            void stop()
            {
                running_ = false;

                struct kevent wake_event{};
                EV_SET(&wake_event, 0, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);

                if (::kevent(kq_.get(), &wake_event, 1, nullptr, 0, nullptr) == -1)
                    throw std::system_error(errno, std::generic_category(), "failed to signal kqueue loop thread termination");
            }

        private:

            Socket kq_;
            std::atomic<bool> running_{false};
            std::unordered_map<int, EventCallback> callbacks_;
    };
} //namespace chronos::transport
#endif //defined(__APPLE__) | defined(__FreeBSD__)