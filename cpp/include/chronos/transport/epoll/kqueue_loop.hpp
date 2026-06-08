#pragma once

#if defined(__APPLE__) | defined(__FreeBSD__)

#include "../socket.hpp"
#include "event_types.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>
#include <stdexcept>

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
                if (!kq_.isValid()) throw std::runtime_error("failed to create kqueue.");

                //kqueue user-event to act as cross-thread wakeup signal
                struct kevent wake_event{};
                EV_SET(&wake_event, 0, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
                ::kevent(kq_.get(), &wake_event, 1, nullptr, 0, nullptr);
            }

            /**
            * @brief register a new fd with the event loop
            */

            void add(int fd, EventFlags events, EventCallback cb)
            {
                callbacks_[fd] = std::move(cb);
                modify(fd, events);
            }

            /**
            * @brief change watched events for an existing fd
            */

            void modify(int fd, EventFlags events)
            {
                std::vector<struct kevent> changes;
                
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

                ::kevent(kq_.get(), changes.data(), static_cast<int>(changes.size()), nullptr, 0, nullptr);
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
                ::kevent(kq_.get(), changes, 2, nullptr, 0, nullptr);
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
                        break;
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

                            it->second(fd, triggered_flags);
                        }
                    }
                }
            }

            void stop()
            {
                running_ = false;

                struct kevent wake_event{};
                EV_SET(&wake_event, 0, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
                ::kevent(kq_.get(), &wake_event, 1, nullptr, 0, nullptr);
            }

        private:

            Socket kq_;
            std::atomic<bool> running_{false};
            std::unordered_map<int, EventCallback> callbacks_;
    };
} //namespace chronos::transport
#endif //defined(__APPLE__) | defined(__FreeBSD__)