#pragma once

#include <affine/executor.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace affine {

/**
 * An executor for a host-owned main/message thread.
 *
 * `dispatch` enqueues a callback on the host thread. `pump`, when supplied,
 * cooperatively processes host events on that same thread until its predicate
 * returns true. Without `pump`, this executor cannot be used with
 * affine::sync_wait.
 */
class main_thread_executor {
public:
    using dispatch_function = std::function<void(std::function<void()>)>;
    using pump_function = std::function<void(std::function<bool()>)>;

    main_thread_executor(std::thread::id owner,
                          dispatch_function dispatch,
                          pump_function pump = {})
        : _state(std::make_shared<state>(
              owner, std::move(dispatch), std::move(pump))) {}

    bool is_current_thread() const noexcept {
        return std::this_thread::get_id() == _state->_owner;
    }

    bool schedule(std::coroutine_handle<> handle) {
        dispatch_function dispatch;
        {
            std::lock_guard lock(_state->_mutex);
            if (!_state->_accepting)
                return false;
            dispatch = _state->_dispatch;
        }

        if (!dispatch)
            return false;

        auto state = _state;
        dispatch([state = std::move(state), handle] {
            if (state->_accepting.load(std::memory_order_acquire))
                handle.resume();
        });
        return true;
    }

    template<class Predicate>
    void run_until(Predicate&& done) {
        if (!is_current_thread())
            throw std::logic_error("main_thread_executor must be pumped on its owner thread");
        if (!_state->_pump)
            throw std::logic_error("main_thread_executor has no host event pump");
        _state->_pump(std::function<bool()>(std::forward<Predicate>(done)));
    }

    void close() noexcept {
        _state->_accepting.store(false, std::memory_order_release);
    }

    [[nodiscard]] bool accepting() const noexcept {
        return _state->_accepting.load(std::memory_order_acquire);
    }

private:
    struct state {
        state(std::thread::id owner,
              dispatch_function dispatch,
              pump_function pump)
            : _owner(owner),
              _dispatch(std::move(dispatch)),
              _pump(std::move(pump)) {}

        std::thread::id _owner;
        dispatch_function _dispatch;
        pump_function _pump;
        std::atomic<bool> _accepting = true;
        std::mutex _mutex;
    };

    std::shared_ptr<state> _state;
};

static_assert(executor<main_thread_executor>);
static_assert(sync_executor<main_thread_executor>);

} // namespace affine
