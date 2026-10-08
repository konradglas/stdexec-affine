#pragma once

#ifndef AFFINE_USE_PORTABLE_STOP_TOKEN
#  if defined(__APPLE__) && defined(__ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__) && __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__ < 110000
#    define AFFINE_USE_PORTABLE_STOP_TOKEN 1
#  else
#    define AFFINE_USE_PORTABLE_STOP_TOKEN 0
#  endif
#endif

#if !AFFINE_USE_PORTABLE_STOP_TOKEN
#include <stop_token>
namespace affine {
using std::stop_token;
using std::stop_source;
using std::stop_callback;
}
#else
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace affine {
namespace detail {
struct stop_callback_base {
    virtual ~stop_callback_base() = default;
    virtual void invoke() noexcept = 0;
    std::thread::id executing;
    bool* destroyed = nullptr;
    stop_callback_base* previous = nullptr;
    stop_callback_base* next = nullptr;
};

struct stop_state {
    std::atomic<bool> requested{false};
    std::atomic<unsigned> sources{1};
    std::mutex mutex;
    std::condition_variable completed;
    stop_callback_base* callbacks = nullptr;

    void remove(stop_callback_base* callback) noexcept {
        if (callback->previous) {
            callback->previous->next = callback->next;
        } else if (callbacks == callback) {
            callbacks = callback->next;
        } else {
            return;
        }
        if (callback->next) {
            callback->next->previous = callback->previous;
        }
        callback->previous = callback->next = nullptr;
    }
};
}

template<class Callback> class stop_callback;

class stop_token {
public:
    stop_token() noexcept = default;
    bool stop_requested() const noexcept {
        return _state && _state->requested.load(std::memory_order_acquire);
    }
    bool stop_possible() const noexcept {
        return _state && (stop_requested() || _state->sources.load(std::memory_order_acquire) != 0);
    }
    friend bool operator==(const stop_token&, const stop_token&) = default;

private:
    explicit stop_token(std::shared_ptr<detail::stop_state> state) noexcept : _state(std::move(state)) {}
    std::shared_ptr<detail::stop_state> _state;
    friend class stop_source;
    template<class Callback> friend class stop_callback;
};

class stop_source {
public:
    stop_source() : _state(std::make_shared<detail::stop_state>()) {}
    stop_source(const stop_source& other) noexcept : _state(other._state) {
        if (_state) {
            _state->sources.fetch_add(1, std::memory_order_relaxed);
        }
    }
    stop_source(stop_source&&) noexcept = default;
    stop_source& operator=(stop_source other) noexcept {
        _state.swap(other._state);
        return *this;
    }
    ~stop_source() {
        if (_state) {
            _state->sources.fetch_sub(1, std::memory_order_release);
        }
    }
    affine::stop_token get_token() const noexcept { return affine::stop_token(_state); }
    bool stop_requested() const noexcept { return get_token().stop_requested(); }
    bool stop_possible() const noexcept { return bool(_state); }
    bool request_stop() noexcept {
        // A callback may destroy the source, its registration, or another callback.
        auto state = _state;
        if (!state) {
            return false;
        }
        std::unique_lock lock(state->mutex);
        if (state->requested.exchange(true, std::memory_order_acq_rel)) {
            return false;
        }
        while (auto* callback = state->callbacks) {
            state->remove(callback);
            bool destroyed = false;
            callback->destroyed = &destroyed;
            callback->executing = std::this_thread::get_id();
            lock.unlock();
            callback->invoke();
            lock.lock();
            if (!destroyed) {
                callback->executing = {};
                callback->destroyed = nullptr;
            }
            state->completed.notify_all();
        }
        return true;
    }

private:
    std::shared_ptr<detail::stop_state> _state;
};

template<class Callback>
class stop_callback : private detail::stop_callback_base {
public:
    template<class Init>
    explicit stop_callback(affine::stop_token token, Init&& callback)
        : _state(std::move(token._state)), _callback(std::forward<Init>(callback)) {
        auto state = _state;
        if (!state) {
            return;
        }
        std::unique_lock lock(state->mutex);
        if (!state->requested.load(std::memory_order_acquire)) {
            next = state->callbacks;
            if (next) {
                next->previous = this;
            }
            state->callbacks = this;
            return;
        }
        lock.unlock();
        invoke();
    }
    stop_callback(const stop_callback&) = delete;
    stop_callback& operator=(const stop_callback&) = delete;
    ~stop_callback() {
        if (!_state) {
            return;
        }
        std::unique_lock lock(_state->mutex);
        _state->remove(this);
        if (executing == std::this_thread::get_id()) {
            // The requester must not touch this registration after invocation returns.
            *destroyed = true;
        } else {
            _state->completed.wait(lock, [&] { return executing == std::thread::id{}; });
        }
    }

private:
    void invoke() noexcept override { std::forward<Callback>(_callback)(); }
    std::shared_ptr<detail::stop_state> _state;
    Callback _callback;
};

template<class Callback>
stop_callback(stop_token, Callback) -> stop_callback<Callback>;
}
#endif
