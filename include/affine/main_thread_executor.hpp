#pragma once

#include <affine/executor.hpp>

#include <algorithm>
#include <atomic>
#include <condition_variable>
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
    using current_thread_function = std::function<bool()>;
    using dispatch_function = std::function<void(std::function<void()>)>;
    using pump_function = std::function<void(std::function<bool()>)>;

    main_thread_executor(std::thread::id owner,
                          dispatch_function dispatch,
                          pump_function pump = {})
        : main_thread_executor(
              [owner] { return std::this_thread::get_id() == owner; },
              std::move(dispatch),
              std::move(pump)) {}

    main_thread_executor(current_thread_function is_current,
                         dispatch_function dispatch,
                         pump_function pump = {})
        : _state(std::make_shared<state>(
              std::move(is_current), std::move(dispatch), std::move(pump))) {}

    bool is_current_thread() const noexcept {
        return _state->_is_current() || _draining == _state.get();
    }

    bool schedule(std::coroutine_handle<> handle) {
        return schedule_callback([handle] { handle.resume(); });
    }

    bool schedule_callback(std::function<void()> callback) {
        dispatch_function dispatch;
        auto record = std::make_shared<callback_record>(std::move(callback));
        {
            std::lock_guard lock(_state->_mutex);
            if (!_state->_accepting) {
                _state->_ready.push_back(std::move(record));
                _state->_condition.notify_all();
                return true;
            }
            dispatch = _state->_dispatch;
            _state->_pending.push_back(record);
        }
        _state->_condition.notify_all();

        if (!dispatch) {
            remove_pending(record);
            return false;
        }

        auto state = _state;
        auto dispatched_record = record;
        try {
            dispatch([state = std::move(state), record = std::move(dispatched_record)] {
                std::lock_guard execution(state->_execution);
                if (record->claim()) {
                    state->remove_pending(record);
                    auto callback = std::move(record->_callback);
                    callback();
                }
            });
        } catch (...) {
            remove_pending(record);
            return false;
        }
        return true;
    }

    // Explicit takeover is for callers that can safely execute the entire
    // resumed task tree on this thread (e.g. exclusive owner teardown).
    void drain(bool allow_non_owner = false) {
        if (!is_current_thread() && !allow_non_owner)
            throw std::logic_error("main_thread_executor must drain on its owner thread");

        std::lock_guard execution(_state->_execution);
        drain_scope scope(_state.get());
        for (;;) {
            auto record = _state->take_next();
            if (!record)
                return;
            auto callback = std::move(record->_callback);
            callback();
        }
    }

    template<class Predicate>
    void run_until(Predicate&& done, bool allow_non_owner = false) {
        if (!is_current_thread() && !allow_non_owner)
            throw std::logic_error("main_thread_executor must be pumped on its owner thread");
        std::function<bool()> done_function = std::forward<Predicate>(done);
        std::function<bool()> predicate = [this, &done_function] {
            return done_function() || _state->has_work();
        };
        while (!done_function()) {
            drain(allow_non_owner);
            if (done_function())
                return;
            if (_state->_is_current() && _state->_accepting.load(std::memory_order_acquire) && _state->_pump) {
                _state->_pump(predicate);
            } else {
                _state->wait_until_work(done_function);
            }
        }
    }

    void close() noexcept {
        {
            std::lock_guard lock(_state->_mutex);
            if (!_state->_accepting.exchange(false, std::memory_order_acq_rel))
                return;
            for (auto& record : _state->_pending) {
                if (record->claim())
                    _state->_ready.push_back(std::move(record));
            }
            _state->_pending.clear();
            _state->_dispatch = {};
        }
        _state->_condition.notify_all();
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
        state(current_thread_function is_current,
              dispatch_function dispatch,
              pump_function pump)
            : _is_current(std::move(is_current)),
              _dispatch(std::move(dispatch)),
              _pump(std::move(pump)) {}

        // Serializes dispatched callbacks with explicit joining-thread drains.
        std::recursive_mutex _execution;
        current_thread_function _is_current;
        dispatch_function _dispatch;
        pump_function _pump;
        std::atomic<bool> _accepting = true;
        mutable std::mutex _mutex;
        std::condition_variable _condition;
        std::vector<std::shared_ptr<callback_record>> _pending;
        std::vector<std::shared_ptr<callback_record>> _ready;

        bool has_work() const {
            std::lock_guard lock(_mutex);
            if (!_ready.empty())
                return true;
            return std::ranges::any_of(_pending, [](const auto& record) {
                return record && !record->_claimed.load(std::memory_order_acquire);
            });
        }

        void wait_until_work(const std::function<bool()>& done) {
            std::unique_lock lock(_mutex);
            _condition.wait(lock, [&] {
                if (done() || !_ready.empty())
                    return true;
                return std::ranges::any_of(_pending, [](const auto& record) {
                    return record && !record->_claimed.load(std::memory_order_acquire);
                });
            });
        }

        std::shared_ptr<callback_record> take_next() {
            std::lock_guard lock(_mutex);
            if (!_ready.empty()) {
                auto record = std::move(_ready.back());
                _ready.pop_back();
                return record;
            }

            while (!_pending.empty()) {
                auto record = std::move(_pending.back());
                _pending.pop_back();
                if (record && record->claim())
                    return record;
            }
            return {};
        }

        void remove_pending(const std::shared_ptr<callback_record>& record) {
            std::lock_guard lock(_mutex);
            std::erase(_pending, record);
        }
    };

    inline static thread_local state* _draining = nullptr;
    struct drain_scope {
        explicit drain_scope(state* value) : previous(std::exchange(_draining, value)) {}
        ~drain_scope() { _draining = previous; }
        state* previous;
    };
    std::shared_ptr<state> _state;

    void remove_pending(const std::shared_ptr<callback_record>& record) {
        _state->remove_pending(record);
    }
};

static_assert(executor<main_thread_executor>);
static_assert(drainable_executor<main_thread_executor>);

} // namespace affine
