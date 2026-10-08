#pragma once

#include <condition_variable>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <affine/stop_token.hpp>
#include <type_traits>
#include <utility>

#include <affine/executor.hpp>

namespace affine {

template<class T, class Executor>
class affine_task;

template<class T, class Executor>
T sync_wait(affine_task<T, Executor> task);

template<class Executor>
void sync_wait(affine_task<void, Executor> task);

namespace this_task {

struct stop_token_t {};
inline constexpr stop_token_t stop_token{};

} // namespace this_task

namespace detail {

struct empty_env {};

enum class completion_status {
    running,
    succeeded,
    failed,
    cancelled,
};

template<class Awaitable, class Promise>
struct awaitable_adapter {
    static constexpr bool customized = false;
    static constexpr bool preserves_affinity = false;
};

template<class Promise, class = void>
struct environment_adapter {
    static empty_env make(Promise const&) noexcept { return {}; }
};

struct task_state {
    mutable std::mutex _mutex;
    std::condition_variable _completion;
    bool _completed = false;
    completion_status _status = completion_status::running;
    std::exception_ptr _error;
    affine::stop_source _stop_source;
    std::optional<affine::stop_callback<std::function<void()>>> _parent_stop_callback;

    void inherit_stop_token(affine::stop_token token) {
        std::lock_guard lock(_mutex);
        _parent_stop_callback.emplace(token, [this] {
            // Completion can destroy this task inline during stop propagation.
            auto source = _stop_source;
            source.request_stop();
        });
    }

    void complete(std::exception_ptr e = {}) noexcept {
        {
            std::lock_guard lock(_mutex);
            if (_completed)
                return;
            _error = std::move(e);
            _status = _error ? completion_status::failed
                             : completion_status::succeeded;
            _completed = true;
        }
        _completion.notify_all();
    }

    void cancel() noexcept {
        {
            std::lock_guard lock(_mutex);
            if (_completed)
                return;
            _status = completion_status::cancelled;
            _completed = true;
        }
        _completion.notify_all();
    }

    bool is_completed() const noexcept {
        std::lock_guard lock(_mutex);
        return _completed;
    }

    void wait() {
        std::unique_lock lock(_mutex);
        _completion.wait(lock, [this] { return _completed; });
    }

    completion_status status() const noexcept {
        std::lock_guard lock(_mutex);
        return _status;
    }
};

class operation_cancelled : public std::runtime_error {
public:
    operation_cancelled()
        : std::runtime_error("affine operation was cancelled") {}
};

struct stop_token_awaitable {
    affine::stop_token _token;

    bool await_ready() const noexcept { return true; }
    void await_suspend(std::coroutine_handle<>) const noexcept {}
    affine::stop_token await_resume() const noexcept { return _token; }
};

template<class T>
struct result_storage {
    std::optional<T> _value;
};

template<>
struct result_storage<void> {};

template<class T>
struct task_promise_base : result_storage<T> {
    std::shared_ptr<task_state> _state = std::make_shared<task_state>();

    [[nodiscard]] affine::stop_token stop_token() const noexcept {
        return _state->_stop_source.get_token();
    }

    void unhandled_exception() noexcept {
        _state->complete(std::current_exception());
    }
};

template<class T>
concept has_member_co_await = requires(T&& value) {
    std::forward<T>(value).operator co_await();
};

template<class T>
concept has_adl_co_await = requires(T&& value) {
    operator co_await(std::forward<T>(value));
};

template<class T>
decltype(auto) get_awaiter(T&& value) {
    if constexpr (has_member_co_await<T>)
        return std::forward<T>(value).operator co_await();
    else if constexpr (has_adl_co_await<T>)
        return operator co_await(std::forward<T>(value));
    else
        return std::forward<T>(value);
}

template<class Awaitable>
class stored_awaitable {
    using value_type = std::remove_reference_t<Awaitable>;
    static constexpr bool borrows = std::is_lvalue_reference_v<Awaitable>;
    using storage_type = std::conditional_t<borrows,
                                            std::reference_wrapper<value_type>,
                                            value_type>;

public:
    explicit stored_awaitable(Awaitable&& value)
        : _storage(make_storage(std::forward<Awaitable>(value))) {}

    decltype(auto) get() && {
        if constexpr (borrows)
            return _storage.get();
        else
            return std::move(_storage);
    }

private:
    static storage_type make_storage(Awaitable&& value) {
        if constexpr (borrows)
            return std::ref(value);
        else
            return std::forward<Awaitable>(value);
    }

    storage_type _storage;
};

template<class Executor>
struct continuation_bridge {
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    explicit continuation_bridge(handle_type handle) noexcept : _handle(handle) {}
    continuation_bridge(continuation_bridge&& other) noexcept
        : _handle(std::exchange(other._handle, {})) {}
    continuation_bridge& operator=(continuation_bridge&& other) noexcept {
        if (this != &other) {
            destroy();
            _handle = std::exchange(other._handle, {});
        }
        return *this;
    }
    continuation_bridge(const continuation_bridge&) = delete;
    ~continuation_bridge() { destroy(); }

    [[nodiscard]] handle_type handle() const noexcept { return _handle; }

private:
    void destroy() noexcept {
        if (_handle)
            _handle.destroy();
        _handle = {};
    }

    handle_type _handle{};
};

template<class Executor>
struct continuation_bridge<Executor>::promise_type {
    Executor _executor;
    std::coroutine_handle<> _continuation{};
    affine::stop_token _stop_token;

    promise_type(Executor executor,
                 std::coroutine_handle<> continuation,
                 affine::stop_token stop_token)
        : _executor(std::move(executor)),
          _continuation(continuation),
          _stop_token(stop_token) {}

    [[nodiscard]] affine::stop_token stop_token() const noexcept {
        return _stop_token;
    }

    continuation_bridge get_return_object() noexcept {
        return continuation_bridge{handle_type::from_promise(*this)};
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    void unhandled_exception() const noexcept { std::terminate(); }

    struct final_awaiter {
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(handle_type handle) const noexcept {
            auto& promise = handle.promise();
            if (!promise._continuation)
                return std::noop_coroutine();
            if (promise._executor.is_current_thread())
                return promise._continuation;
            if (!promise._executor.schedule(promise._continuation))
                std::terminate();
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    final_awaiter final_suspend() const noexcept { return {}; }
};

template<class Executor>
continuation_bridge<Executor> make_continuation_bridge(
    Executor executor,
    std::coroutine_handle<> continuation,
    affine::stop_token stop_token) {
    co_return;
}

template<class Awaitable, class Executor>
class affine_awaitable {
    using storage_type = stored_awaitable<Awaitable>;
    using awaiter_type = std::remove_cvref_t<decltype(get_awaiter(
        std::declval<storage_type&&>().get()))>;
    using bridge_type = continuation_bridge<Executor>;

public:
    affine_awaitable(Awaitable&& awaitable,
                     Executor executor,
                     affine::stop_token stop_token)
        : _awaitable(std::forward<Awaitable>(awaitable)),
          _awaiter(get_awaiter(std::move(_awaitable).get())),
          _executor(std::move(executor)),
          _stop_token(stop_token) {}

    bool await_ready() noexcept(noexcept(_awaiter.await_ready())) {
        return _awaiter.await_ready();
    }

    auto await_suspend(std::coroutine_handle<> continuation) {
        _bridge.emplace(make_continuation_bridge(
            _executor, continuation, _stop_token));
        auto bridge_handle = _bridge->handle();

        using result_type = decltype(_awaiter.await_suspend(bridge_handle));
        if constexpr (std::is_void_v<result_type>) {
            _awaiter.await_suspend(bridge_handle);
        } else if constexpr (std::is_same_v<result_type, bool>) {
            if (!_awaiter.await_suspend(bridge_handle)) {
                _bridge.reset();
                return false;
            }
            return true;
        } else {
            return _awaiter.await_suspend(bridge_handle);
        }
    }

    decltype(auto) await_resume() {
        return _awaiter.await_resume();
    }

private:
    storage_type _awaitable;
    awaiter_type _awaiter;
    Executor _executor;
    affine::stop_token _stop_token;
    std::optional<bridge_type> _bridge;
};

} // namespace detail

using operation_cancelled = detail::operation_cancelled;

/**
 * A coroutine task whose execution and continuations are affine to Executor.
 *
 * Executor must provide:
 *
 *   bool is_current_thread() const noexcept;
 *   bool schedule(std::coroutine_handle<>);
 * A drainable Executor additionally provides:
 *
 *   void drain();
 *   void run_until(Predicate&& predicate);
 *
 * Synchronous waiting requires a drainable executor. `run_until` must
 * cooperatively execute queued work on the executor's owner thread.
 */
template<class T, class Executor>
class [[nodiscard]] affine_task {
public:
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    affine_task() noexcept = default;

    explicit affine_task(handle_type handle) noexcept : _handle(handle) {}

    affine_task(affine_task&& other) noexcept
        : _handle(std::exchange(other._handle, {})) {}

    affine_task& operator=(affine_task&& other) noexcept {
        if (this != &other) {
            destroy();
            _handle = std::exchange(other._handle, {});
        }
        return *this;
    }

    affine_task(const affine_task&) = delete;
    affine_task& operator=(const affine_task&) = delete;

    ~affine_task() { destroy(); }

    [[nodiscard]] bool valid() const noexcept { return !!_handle; }

    [[nodiscard]] bool has_executor() const noexcept {
        return _handle && _handle.promise()._executor.has_value();
    }

    [[nodiscard]] bool done() const noexcept {
        return !_handle || _handle.done() || _handle.promise()._state->is_completed();
    }

    [[nodiscard]] std::shared_ptr<detail::task_state> state() const noexcept {
        return _handle ? _handle.promise()._state : nullptr;
    }

    [[nodiscard]] affine::stop_token stop_token() const noexcept {
        return _handle ? _handle.promise()._state->_stop_source.get_token()
                       : affine::stop_token{};
    }

    bool request_stop() noexcept {
        return _handle && _handle.promise()._state->_stop_source.request_stop();
    }

    void inherit_stop_token(affine::stop_token token) {
        if (_handle)
            _handle.promise()._state->inherit_stop_token(token);
    }

    [[nodiscard]] affine_task&& scheduleOn(Executor executor) && {
        bind_executor(std::move(executor), true);
        return std::move(*this);
    }

    [[nodiscard]] Executor& executor() {
        if (!has_executor())
            throw std::logic_error("affine_task is not scheduled on an executor");
        return *_handle.promise()._executor;
    }

    void start() {
        if (!_handle)
            return;

        auto handle = _handle;
        auto& executor = this->executor();
        if (executor.is_current_thread())
            handle.resume();
        else if (!executor.schedule(handle))
            throw std::runtime_error("affine_task executor rejected coroutine");
    }

    struct awaiter {
        explicit awaiter(handle_type value) noexcept : _handle(value) {}
        awaiter(awaiter&& other) noexcept
            : _handle(std::exchange(other._handle, {})) {}
        awaiter(const awaiter&) = delete;
        awaiter& operator=(const awaiter&) = delete;
        ~awaiter() {
            if (_handle)
                _handle.destroy();
        }

        bool await_ready() const noexcept {
            return !_handle || _handle.promise()._state->is_completed();
        }

        template<class ParentPromise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<ParentPromise> continuation) {
            if constexpr (requires { continuation.promise().stop_token(); }) {
                _handle.promise()._state->inherit_stop_token(
                    continuation.promise().stop_token());
            }
            _handle.promise()._continuation = continuation;
            auto& executor = *_handle.promise()._executor;
            if (executor.is_current_thread())
                return _handle;
            if (!executor.schedule(_handle))
                throw std::runtime_error("affine_task executor rejected continuation");
            return std::noop_coroutine();
        }

        T await_resume() {
            auto owner = std::exchange(_handle, {});
            auto& promise = owner.promise();
            if (promise._state->status() == detail::completion_status::cancelled) {
                owner.destroy();
                throw detail::operation_cancelled{};
            }
            if (promise._state->_error) {
                auto error = promise._state->_error;
                owner.destroy();
                std::rethrow_exception(error);
            }
            if (!promise._value) {
                owner.destroy();
                throw std::logic_error("affine task completed without a value");
            }
            T result = std::move(*promise._value);
            owner.destroy();
            return result;
        }

        handle_type _handle;
    };

private:
    template<class, class>
    friend class affine_task;

    [[nodiscard]] bool explicitly_scheduled() const noexcept {
        return _handle && _handle.promise()._explicitly_scheduled;
    }

    void bind_executor(Executor executor, bool explicit_schedule = false) {
        if (_handle) {
            _handle.promise()._executor.emplace(std::move(executor));
            _handle.promise()._explicitly_scheduled = explicit_schedule;
        }
    }

    awaiter operator co_await() && noexcept {
        return awaiter{std::exchange(_handle, {})};
    }

    friend T sync_wait<T, Executor>(affine_task<T, Executor>);

    void destroy() noexcept {
        if (_handle)
            _handle.destroy();
        _handle = {};
    }

    handle_type _handle{};
};

template<class Executor>
class affine_task<void, Executor> {
public:
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    affine_task() noexcept = default;
    explicit affine_task(handle_type handle) noexcept : _handle(handle) {}
    affine_task(affine_task&& other) noexcept : _handle(std::exchange(other._handle, {})) {}
    affine_task& operator=(affine_task&& other) noexcept {
        if (this != &other) {
            destroy();
            _handle = std::exchange(other._handle, {});
        }
        return *this;
    }
    affine_task(const affine_task&) = delete;
    affine_task& operator=(const affine_task&) = delete;
    ~affine_task() { destroy(); }

    [[nodiscard]] bool valid() const noexcept { return !!_handle; }
    [[nodiscard]] bool has_executor() const noexcept {
        return _handle && _handle.promise()._executor.has_value();
    }
    [[nodiscard]] bool done() const noexcept {
        return !_handle || _handle.done() || _handle.promise()._state->is_completed();
    }
    [[nodiscard]] std::shared_ptr<detail::task_state> state() const noexcept {
        return _handle ? _handle.promise()._state : nullptr;
    }

    [[nodiscard]] affine::stop_token stop_token() const noexcept {
        return _handle ? _handle.promise()._state->_stop_source.get_token()
                       : affine::stop_token{};
    }

    bool request_stop() noexcept {
        return _handle && _handle.promise()._state->_stop_source.request_stop();
    }

    void inherit_stop_token(affine::stop_token token) {
        if (_handle)
            _handle.promise()._state->inherit_stop_token(token);
    }

    [[nodiscard]] affine_task&& scheduleOn(Executor executor) && {
        bind_executor(std::move(executor), true);
        return std::move(*this);
    }

    [[nodiscard]] Executor& executor() {
        if (!has_executor())
            throw std::logic_error("affine_task is not scheduled on an executor");
        return *_handle.promise()._executor;
    }

    void start() {
        if (!_handle)
            return;
        auto handle = _handle;
        auto& executor = this->executor();
        if (executor.is_current_thread())
            handle.resume();
        else if (!executor.schedule(handle))
            throw std::runtime_error("affine_task executor rejected coroutine");
    }

    struct awaiter {
        explicit awaiter(handle_type value) noexcept : _handle(value) {}
        awaiter(awaiter&& other) noexcept
            : _handle(std::exchange(other._handle, {})) {}
        awaiter(const awaiter&) = delete;
        awaiter& operator=(const awaiter&) = delete;
        ~awaiter() {
            if (_handle)
                _handle.destroy();
        }

        bool await_ready() const noexcept {
            return !_handle || _handle.promise()._state->is_completed();
        }
        template<class ParentPromise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<ParentPromise> continuation) {
            if constexpr (requires { continuation.promise().stop_token(); }) {
                _handle.promise()._state->inherit_stop_token(
                    continuation.promise().stop_token());
            }
            _handle.promise()._continuation = continuation;
            auto& executor = *_handle.promise()._executor;
            if (executor.is_current_thread())
                return _handle;
            if (!executor.schedule(_handle))
                throw std::runtime_error("affine_task executor rejected continuation");
            return std::noop_coroutine();
        }
        void await_resume() {
            auto owner = std::exchange(_handle, {});
            auto& promise = owner.promise();
            if (promise._state->status() == detail::completion_status::cancelled) {
                owner.destroy();
                throw detail::operation_cancelled{};
            }
            if (promise._state->_error) {
                auto error = promise._state->_error;
                owner.destroy();
                std::rethrow_exception(error);
            }
            owner.destroy();
        }

        handle_type _handle;
    };

private:
    template<class, class>
    friend class affine_task;

    [[nodiscard]] bool explicitly_scheduled() const noexcept {
        return _handle && _handle.promise()._explicitly_scheduled;
    }

    void bind_executor(Executor executor, bool explicit_schedule = false) {
        if (_handle) {
            _handle.promise()._executor.emplace(std::move(executor));
            _handle.promise()._explicitly_scheduled = explicit_schedule;
        }
    }

    awaiter operator co_await() && noexcept {
        return awaiter{std::exchange(_handle, {})};
    }

    friend void sync_wait<Executor>(affine_task<void, Executor>);

    void destroy() noexcept {
        if (_handle)
            _handle.destroy();
        _handle = {};
    }
    handle_type _handle{};
};

template<class T, class Executor>
struct affine_task<T, Executor>::promise_type : detail::task_promise_base<T> {
    std::optional<Executor> _executor;
    std::coroutine_handle<> _continuation{};
    bool _explicitly_scheduled = false;

    promise_type() {
        if constexpr (std::default_initializable<Executor>)
            _executor.emplace();
    }

    template<class First, class... Rest>
    explicit promise_type(First&&, Rest&&...) : promise_type() {}

    affine_task get_return_object() noexcept {
        return affine_task{handle_type::from_promise(*this)};
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }

    struct final_awaiter {
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(handle_type handle) const noexcept {
            auto& promise = handle.promise();
            promise._state->complete(promise._state->_error);
            if (!promise._continuation)
                return std::noop_coroutine();
            auto& executor = *promise._executor;
            if (executor.is_current_thread())
                return promise._continuation;
            if (!executor.schedule(promise._continuation))
                std::terminate();
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    final_awaiter final_suspend() const noexcept { return {}; }
    template<class U, class ChildExecutor>
    auto await_transform(affine_task<U, ChildExecutor>&& child) {
        if constexpr (std::same_as<ChildExecutor, Executor>) {
            if (!child.explicitly_scheduled())
                child.bind_executor(*_executor);
        } else if (!child.has_executor()) {
            throw std::logic_error(
                "a task on another executor type requires scheduleOn()");
        }
        auto awaiter = std::move(child).operator co_await();
        return detail::affine_awaitable<decltype(awaiter), Executor>{
            std::move(awaiter), *_executor,
            this->_state->_stop_source.get_token()};
    }

    template<class Awaitable>
    auto await_transform(Awaitable&& awaitable) {
        using adapter = detail::awaitable_adapter<
            std::remove_cvref_t<Awaitable>, promise_type>;
        if constexpr (adapter::customized) {
            if constexpr (adapter::preserves_affinity) {
                return adapter::adapt(
                    std::forward<Awaitable>(awaitable), *this, *_executor);
            } else {
                auto adapted = adapter::adapt(
                    std::forward<Awaitable>(awaitable), *this);
                return detail::affine_awaitable<decltype(adapted), Executor>{
                    std::move(adapted), *_executor,
                    this->_state->_stop_source.get_token()};
            }
        } else {
            return detail::affine_awaitable<Awaitable, Executor>{
                std::forward<Awaitable>(awaitable), *_executor,
                this->_state->_stop_source.get_token()};
        }
    }

    auto await_transform(this_task::stop_token_t const&) noexcept {
        return detail::stop_token_awaitable{
            this->_state->_stop_source.get_token()};
    }

    auto get_env() const noexcept {
        return detail::environment_adapter<promise_type>::make(*this);
    }
    std::coroutine_handle<> unhandled_stopped() noexcept {
        this->_state->cancel();
        return _continuation ? _continuation : std::noop_coroutine();
    }
    template<class U> void return_value(U&& value) {
        this->_value.emplace(std::forward<U>(value));
    }
};

template<class Executor>
struct affine_task<void, Executor>::promise_type : detail::task_promise_base<void> {
    std::optional<Executor> _executor;
    std::coroutine_handle<> _continuation{};
    bool _explicitly_scheduled = false;

    promise_type() {
        if constexpr (std::default_initializable<Executor>)
            _executor.emplace();
    }

    template<class First, class... Rest>
    explicit promise_type(First&&, Rest&&...) : promise_type() {}

    affine_task get_return_object() noexcept {
        return affine_task{handle_type::from_promise(*this)};
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }

    struct final_awaiter {
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(handle_type handle) const noexcept {
            auto& promise = handle.promise();
            promise._state->complete(promise._state->_error);
            if (!promise._continuation)
                return std::noop_coroutine();
            auto& executor = *promise._executor;
            if (executor.is_current_thread())
                return promise._continuation;
            if (!executor.schedule(promise._continuation))
                std::terminate();
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    final_awaiter final_suspend() const noexcept { return {}; }
    template<class U, class ChildExecutor>
    auto await_transform(affine_task<U, ChildExecutor>&& child) {
        if constexpr (std::same_as<ChildExecutor, Executor>) {
            if (!child.explicitly_scheduled())
                child.bind_executor(*_executor);
        } else if (!child.has_executor()) {
            throw std::logic_error(
                "a task on another executor type requires scheduleOn()");
        }
        auto awaiter = std::move(child).operator co_await();
        return detail::affine_awaitable<decltype(awaiter), Executor>{
            std::move(awaiter), *_executor,
            this->_state->_stop_source.get_token()};
    }

    template<class Awaitable>
    auto await_transform(Awaitable&& awaitable) {
        using adapter = detail::awaitable_adapter<
            std::remove_cvref_t<Awaitable>, promise_type>;
        if constexpr (adapter::customized) {
            if constexpr (adapter::preserves_affinity) {
                return adapter::adapt(
                    std::forward<Awaitable>(awaitable), *this, *_executor);
            } else {
                auto adapted = adapter::adapt(
                    std::forward<Awaitable>(awaitable), *this);
                return detail::affine_awaitable<decltype(adapted), Executor>{
                    std::move(adapted), *_executor,
                    this->_state->_stop_source.get_token()};
            }
        } else {
            return detail::affine_awaitable<Awaitable, Executor>{
                std::forward<Awaitable>(awaitable), *_executor,
                this->_state->_stop_source.get_token()};
        }
    }

    auto await_transform(this_task::stop_token_t const&) noexcept {
        return detail::stop_token_awaitable{
            this->_state->_stop_source.get_token()};
    }

    auto get_env() const noexcept {
        return detail::environment_adapter<promise_type>::make(*this);
    }
    std::coroutine_handle<> unhandled_stopped() noexcept {
        this->_state->cancel();
        return _continuation ? _continuation : std::noop_coroutine();
    }
    void return_void() const noexcept {}
};

template<class T, class Executor>
T sync_wait(affine_task<T, Executor> task) {
    static_assert(drainable_executor<Executor>,
                  "sync_wait requires a drainable executor");
    auto state = task.state();
    if (!state)
        throw std::invalid_argument("sync_wait requires a valid affine_task");

    auto& executor = task.executor();
    task.start();
    if (executor.is_current_thread())
        executor.run_until([&] { return state->is_completed(); });
    else
        state->wait();

    if (state->status() == detail::completion_status::cancelled)
        throw operation_cancelled{};
    if (state->_error)
        std::rethrow_exception(state->_error);

    // The public task API intentionally keeps ownership opaque; completion is
    // observed here and the task frame is released with the task object.
    return std::move(*task._handle.promise()._value);
}

template<class Executor>
void sync_wait(affine_task<void, Executor> task) {
    static_assert(drainable_executor<Executor>,
                  "sync_wait requires a drainable executor");
    auto state = task.state();
    if (!state)
        throw std::invalid_argument("sync_wait requires a valid affine_task");
    auto& executor = task.executor();
    task.start();
    if (executor.is_current_thread())
        executor.run_until([&] { return state->is_completed(); });
    else
        state->wait();
    if (state->status() == detail::completion_status::cancelled)
        throw operation_cancelled{};
    if (state->_error)
        std::rethrow_exception(state->_error);
}

template<class T, class Executor>
using main_actor_task = affine_task<T, Executor>;

} // namespace affine
