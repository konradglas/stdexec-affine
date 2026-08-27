#pragma once

#include <affine/executor.hpp>

#include <condition_variable>
#include <coroutine>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace affine {

namespace detail {

class serial_executor_state {
public:
    bool is_current_thread() const noexcept {
        std::lock_guard lock(_mutex);
        return _started && std::this_thread::get_id() == _owner;
    }

    bool schedule(std::coroutine_handle<> handle) {
        {
            std::lock_guard lock(_mutex);
            if (_stopping)
                return false;
            _ready.push_back(handle);
        }
        _condition.notify_one();
        return true;
    }

    void drain() {
        require_owner_thread();
        while (auto handle = take_ready())
            handle.resume();
    }

    template<class Predicate>
    void run_until(Predicate&& done) {
        require_owner_thread();
        while (!done()) {
            std::coroutine_handle<> handle;
            {
                std::unique_lock lock(_mutex);
                _condition.wait(lock, [&] {
                    return done() || !_ready.empty() || _stopping;
                });
                if (done())
                    return;
                if (_ready.empty())
                    throw std::logic_error(
                        "serial executor stopped before awaited work completed");
                handle = _ready.front();
                _ready.pop_front();
            }
            handle.resume();
        }
    }

    void run() {
        {
            std::lock_guard lock(_mutex);
            _owner = std::this_thread::get_id();
            _started = true;
        }
        _condition.notify_all();

        for (;;) {
            std::coroutine_handle<> handle;
            {
                std::unique_lock lock(_mutex);
                _condition.wait(lock, [&] {
                    return !_ready.empty() || _stopping;
                });
                if (_ready.empty())
                    return;
                handle = _ready.front();
                _ready.pop_front();
            }
            handle.resume();
        }
    }

    void wait_until_started() {
        std::unique_lock lock(_mutex);
        _condition.wait(lock, [&] { return _started; });
    }

    void request_stop() noexcept {
        {
            std::lock_guard lock(_mutex);
            _stopping = true;
        }
        _condition.notify_all();
    }

private:
    std::coroutine_handle<> take_ready() {
        std::lock_guard lock(_mutex);
        if (_ready.empty())
            return {};
        auto handle = _ready.front();
        _ready.pop_front();
        return handle;
    }

    void require_owner_thread() const {
        if (!is_current_thread())
            throw std::logic_error(
                "serial executor must be drained on its owner thread");
    }

    mutable std::mutex _mutex;
    std::condition_variable _condition;
    std::deque<std::coroutine_handle<>> _ready;
    std::thread::id _owner;
    bool _started = false;
    bool _stopping = false;
};

} // namespace detail

class serial_executor {
public:
    bool is_current_thread() const noexcept {
        return _state->is_current_thread();
    }

    bool schedule(std::coroutine_handle<> handle) {
        return _state->schedule(handle);
    }

    void drain() { _state->drain(); }

    template<class Predicate>
    void run_until(Predicate&& done) {
        _state->run_until(std::forward<Predicate>(done));
    }

private:
    explicit serial_executor(std::shared_ptr<detail::serial_executor_state> state)
        : _state(std::move(state)) {}

    friend class single_thread_context;

    std::shared_ptr<detail::serial_executor_state> _state;
};

class single_thread_context {
public:
    single_thread_context()
        : _state(std::make_shared<detail::serial_executor_state>()),
          _thread([state = _state] { state->run(); }) {
        _state->wait_until_started();
    }

    single_thread_context(const single_thread_context&) = delete;
    single_thread_context& operator=(const single_thread_context&) = delete;

    ~single_thread_context() {
        if (!_thread.joinable())
            return;
        if (std::this_thread::get_id() == _thread.get_id())
            std::terminate();
        _state->request_stop();
        _thread.join();
    }

    [[nodiscard]] serial_executor get_executor() const {
        return serial_executor{_state};
    }

    void shutdown_and_wait() {
        if (!_thread.joinable())
            return;
        if (std::this_thread::get_id() == _thread.get_id())
            throw std::logic_error(
                "single_thread_context cannot join its own thread");
        _state->request_stop();
        _thread.join();
    }

private:
    std::shared_ptr<detail::serial_executor_state> _state;
    std::thread _thread;
};

static_assert(executor<serial_executor>);
static_assert(drainable_executor<serial_executor>);

} // namespace affine
