#pragma once

#if defined(__APPLE__) || defined(__FreeBSD__)

#include "../socket.hpp"
#include "event_types.hpp"

#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <vector>
#include <stdexcept>
#include <string>
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

                ready_batch_.reserve(64); //keeps noexcept run() loop steady in allocation free ss
            }

            /**
            * @brief register a new fd with the event loop
            */

            void add(int fd, EventFlags events, EventCallback cb)
            {

                if (fd < 0) throw std::invalid_argument("invalid file descriptor");

                //enforce epoll's EEXIST contract in user-space
                //EV_ADD is upsert in kqueue, manually prevent double-adds
                if (static_cast<size_t>(fd) >= callbacks_.size())
                    callbacks_.resize(fd + 1); 
                
                if (callbacks_[fd])
                    throw std::system_error(EEXIST, std::generic_category(), "kqueue add failure: fd " + std::to_string(fd) + " already exists");

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

                struct kevent changes[2];
                EV_SET(&changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
                EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);

                if (::kevent(kq_.get(), changes, 2, nullptr, 0, nullptr) == -1)
                {
                    if (errno != ENOENT && errno != EBADF)
                        throw std::system_error(errno, std::generic_category(), "kevent remove failed for fd " + std::to_string(fd));
                }

                if (fd >= 0 && static_cast<size_t>(fd) < callbacks_.size() && callbacks_[fd])
                    callbacks_[fd] = nullptr; // only invoked on success or suppressed errno
            }

            /**
            * @brief blocks current thread waiting for dispatching i/o events
            */

            [[nodiscard]] std::expected<void, std::error_code> run() noexcept
            {
                running_ = true;
                std::vector<struct kevent> events(64);

                while (running_)
                {
                    int num_events = ::kevent(kq_.get(), nullptr, 0, events.data(), static_cast<int>(events.size()), nullptr);

                    if (num_events == -1)
                    {
                        if (errno == EINTR) continue;
                        return std::unexpected(std::error_code(errno, std::generic_category()));
                    }

                    ready_batch_.clear();

                    for (auto i{0}; i < num_events; ++i)
                    {
                        const auto &ev = events[i];

                        if (ev.filter == EVFILT_USER) continue; //thread wakeup signal (stop() was called)

                        int fd = static_cast<int>(ev.ident);

                        EventFlags triggered_flags{EventFlags::None};

                        if (ev.filter == EVFILT_READ) triggered_flags |= EventFlags::Read;
                        if (ev.filter == EVFILT_WRITE) triggered_flags |= EventFlags::Write;
                        if (ev.flags & (EV_EOF | EV_ERROR)) triggered_flags |= EventFlags::Error;

                        //linear scan - avoid map allocations for N <= 64
                        bool found{false};
                        for (auto &pair : ready_batch_)
                        {
                            if (pair.first == fd)
                            {
                                pair.second |= triggered_flags;
                                found = true;
                                break;
                            }
                        }

                        if (!found)
                        {
                            ready_batch_.push_back({fd, triggered_flags});
                        }
                    }

                    for (const auto &[fd, triggered_flags] : ready_batch_)
                    {
                        if (fd >= 0 && static_cast<size_t>(fd) < callbacks_.size() && callbacks_[fd])
                            {
                                try
                                {
                                    EventCallback cb = callbacks_[fd];
                                    cb(fd, triggered_flags);
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

                struct kevent wake_event{};
                EV_SET(&wake_event, 0, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);

                if (::kevent(kq_.get(), &wake_event, 1, nullptr, 0, nullptr) == -1)
                    throw std::system_error(errno, std::generic_category(), "failed to signal kqueue loop thread termination");
            }

        private:

            Socket kq_;
            std::atomic<bool> running_{false};
            std::vector<EventCallback> callbacks_;

            // kqueue delivers READ and WRITE as separate events for the same
            // fd, unlike epoll, which merges them into a single bitmask
            // coalescing before dispatch means callback fires once per fd per tick
            std::vector<std::pair<int, EventFlags>> ready_batch_; //buffer coalescing
    };
} //namespace chronos::transport
#endif //defined(__APPLE__) || defined(__FreeBSD__)