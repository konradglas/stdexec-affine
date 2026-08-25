#pragma once

#include <affine/executor.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

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
        return schedule_callback([handle] { handle.resume(); });
    }

    bool schedule_callback(std::function<void()> callback) {
        dispatch_function dispatch;
        auto record = std::make_shared<callback_record>(std::move(callback));
        {
            std::lock_guard lock(_state->_mutex);
            if (!_state->_accepting)
                return false;
            dispatch = _state->_dispatch;
            _state->_pending.push_back(record);
        }

        if (!dispatch) {
            remove_pending(record);
            return false;
        }

        auto state = _state;
        dispatch([state = std::move(state), record = std::move(record)] {
            if (record->claim())
                record->_callback();
        });
        return true;
    }

    void drain() {
        if (!is_current_thread())
            throw std::logic_error("main_thread_executor must drain on its owner thread");

        for (;;) {
            std::shared_ptr<callback_record> record;
            {
                std::lock_guard lock(_state->_mutex);
                if (_state->_ready.empty())
                    return;
                record = std::move(_state->_ready.back());
                _state->_ready.pop_back();
            }
            record->_callback();
        }
    }

    template<class Predicate>
    void run_until(Predicate&& done) {
        if (!is_current_thread())
            throw std::logic_error("main_thread_executor must be pumped on its owner thread");
        if (!_state->_pump)
            throw std::logic_error("main_thread_executor has no host event pump");
        std::function<bool()> done_function = std::forward<Predicate>(done);
        std::function<bool()> predicate = [this, &done_function] {
            return done_function() || _state->has_ready();
        };
        while (!done_function()) {
            drain();
            if (done_function())
                return;
            _state->_pump(predicate);
        }
    }

    void close() noexcept {
        dispatch_function dispatch;
        {
            std::lock_guard lock(_state->_mutex);
            if (!_state->_accepting.exchange(false, std::memory_order_acq_rel))
                return;
            dispatch = _state->_dispatch;
            for (auto& record : _state->_pending) {
                if (record->claim())
                    _state->_ready.push_back(std::move(record));
            }
            _state->_pending.clear();
        }
        if (dispatch) {
            try {
                dispatch([state = _state] {});
            } catch (...) {
            }
        }
    }

    [[nodiscard]] bool accepting() const noexcept {
        return _state->_accepting.load(std::memory_order_acquire);
    }

private:
    struct callback_record {
        explicit callback_record(std::function<void()> callback)
            : _callback(std::move(callback)) {}

        bool claim() noexcept {
            return !_claimed.exchange(true, std::memory_order_acq_rel);
        }

        std::function<void()> _callback;
        std::atomic<bool> _claimed = false;
    };

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
        mutable std::mutex _mutex;
        std::vector<std::shared_ptr<callback_record>> _pending;
        std::vector<std::shared_ptr<callback_record>> _ready;

        bool has_ready() const {
            std::lock_guard lock(_mutex);
            return !_ready.empty();
        }
    };

    std::shared_ptr<state> _state;

    void remove_pending(const std::shared_ptr<callback_record>& record) {
        std::lock_guard lock(_state->_mutex);
        std::erase(_state->_pending, record);
    }
};

static_assert(executor<main_thread_executor>);
static_assert(sync_executor<main_thread_executor>);
static_assert(drainable_executor<main_thread_executor>);

} // namespace affine
