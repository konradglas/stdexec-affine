#pragma once

#include <condition_variable>
#include <coroutine>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace exec {

template<class T, class Executor>
class affine_task;

template<class T, class Executor>
T sync_wait(affine_task<T, Executor> task);

template<class Executor>
void sync_wait(affine_task<void, Executor> task);

namespace detail {

struct task_state {
    mutable std::mutex mutex;
    std::condition_variable completion;
    bool completed = false;
    std::exception_ptr error;

    void complete(std::exception_ptr e = {}) noexcept {
        {
            std::lock_guard lock(mutex);
            error = std::move(e);
            completed = true;
        }
        completion.notify_all();
    }

    bool is_completed() const noexcept {
        std::lock_guard lock(mutex);
        return completed;
    }
};

template<class T>
struct result_storage {
    std::optional<T> value;
};

template<>
struct result_storage<void> {};

template<class T>
struct task_promise_base : result_storage<T> {
    std::shared_ptr<task_state> state = std::make_shared<task_state>();

    void unhandled_exception() noexcept {
        state->complete(std::current_exception());
    }
};

template<class Awaitable>
concept awaitable = requires(Awaitable&& value) {
    std::forward<Awaitable>(value).await_ready();
};

} // namespace detail

/**
 * A coroutine task whose execution and continuations are affine to Executor.
 *
 * Executor must provide:
 *
 *   bool is_current_thread() const noexcept;
 *   bool schedule(std::coroutine_handle<>);
 *   void run_until(Predicate&& predicate);
 *
 * `run_until` is only required when synchronous waiting is used. It must
 * cooperatively execute work belonging to the executor on its owner thread.
 */
template<class T, class Executor>
class [[nodiscard]] affine_task {
public:
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    affine_task() noexcept = default;

    explicit affine_task(handle_type handle) noexcept : handle_(handle) {}

    affine_task(affine_task&& other) noexcept
        : handle_(std::exchange(other.handle_, {})) {}

    affine_task& operator=(affine_task&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    affine_task(const affine_task&) = delete;
    affine_task& operator=(const affine_task&) = delete;

    ~affine_task() { destroy(); }

    [[nodiscard]] bool valid() const noexcept { return !!handle_; }

    [[nodiscard]] bool done() const noexcept {
        return !handle_ || handle_.done() || handle_.promise().state->is_completed();
    }

    [[nodiscard]] std::shared_ptr<detail::task_state> state() const noexcept {
        return handle_ ? handle_.promise().state : nullptr;
    }

    [[nodiscard]] Executor& executor() {
        if (!handle_)
            throw std::logic_error("affine_task has no executor");
        return handle_.promise().executor;
    }

    void start() {
        if (!handle_)
            return;

        auto handle = handle_;
        auto& executor = handle.promise().executor;
        if (executor.is_current_thread())
            handle.resume();
        else if (!executor.schedule(handle))
            throw std::runtime_error("affine_task executor rejected coroutine");
    }

    struct awaiter {
        handle_type handle;

        bool await_ready() const noexcept {
            return !handle || handle.promise().state->is_completed();
        }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) {
            handle.promise().continuation = continuation;
            auto& executor = handle.promise().executor;
            if (executor.is_current_thread())
                return handle;
            if (!executor.schedule(handle))
                throw std::runtime_error("affine_task executor rejected continuation");
            return std::noop_coroutine();
        }

        T await_resume() {
            auto owner = std::exchange(handle, {});
            auto& promise = owner.promise();
            if (promise.state->error)
                std::rethrow_exception(promise.state->error);
            T result = std::move(*promise.value);
            owner.destroy();
            return result;
        }
    };

    awaiter operator co_await() && noexcept { return {std::exchange(handle_, {})}; }

private:
    friend T sync_wait<T, Executor>(affine_task<T, Executor>);

    void destroy() noexcept {
        if (handle_)
            handle_.destroy();
        handle_ = {};
    }

    handle_type handle_{};
};

template<class Executor>
class affine_task<void, Executor> {
public:
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    affine_task() noexcept = default;
    explicit affine_task(handle_type handle) noexcept : handle_(handle) {}
    affine_task(affine_task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    affine_task& operator=(affine_task&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    affine_task(const affine_task&) = delete;
    affine_task& operator=(const affine_task&) = delete;
    ~affine_task() { destroy(); }

    [[nodiscard]] bool valid() const noexcept { return !!handle_; }
    [[nodiscard]] bool done() const noexcept {
        return !handle_ || handle_.done() || handle_.promise().state->is_completed();
    }
    [[nodiscard]] std::shared_ptr<detail::task_state> state() const noexcept {
        return handle_ ? handle_.promise().state : nullptr;
    }

    [[nodiscard]] Executor& executor() {
        if (!handle_)
            throw std::logic_error("affine_task has no executor");
        return handle_.promise().executor;
    }

    void start() {
        if (!handle_)
            return;
        auto handle = handle_;
        auto& executor = handle.promise().executor;
        if (executor.is_current_thread())
            handle.resume();
        else if (!executor.schedule(handle))
            throw std::runtime_error("affine_task executor rejected coroutine");
    }

    struct awaiter {
        handle_type handle;
        bool await_ready() const noexcept {
            return !handle || handle.promise().state->is_completed();
        }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) {
            handle.promise().continuation = continuation;
            auto& executor = handle.promise().executor;
            if (executor.is_current_thread())
                return handle;
            if (!executor.schedule(handle))
                throw std::runtime_error("affine_task executor rejected continuation");
            return std::noop_coroutine();
        }
        void await_resume() {
            auto owner = std::exchange(handle, {});
            auto& promise = owner.promise();
            if (promise.state->error)
                std::rethrow_exception(promise.state->error);
            owner.destroy();
        }
    };

    awaiter operator co_await() && noexcept { return {std::exchange(handle_, {})}; }

private:
    friend void sync_wait<Executor>(affine_task<void, Executor>);

    void destroy() noexcept {
        if (handle_)
            handle_.destroy();
        handle_ = {};
    }
    handle_type handle_{};
};

template<class T, class Executor>
struct affine_task<T, Executor>::promise_type : detail::task_promise_base<T> {
    Executor executor;
    std::coroutine_handle<> continuation{};

    template<class... Args>
    explicit promise_type(Executor executor, Args&&...)
        : executor(std::move(executor)) {}

    affine_task get_return_object() noexcept {
        return affine_task{handle_type::from_promise(*this)};
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }

    struct final_awaiter {
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(handle_type handle) const noexcept {
            auto& promise = handle.promise();
            promise.state->complete(promise.state->error);
            if (!promise.continuation)
                return std::noop_coroutine();
            if (promise.executor.is_current_thread())
                return promise.continuation;
            if (!promise.executor.schedule(promise.continuation))
                std::terminate();
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    final_awaiter final_suspend() const noexcept { return {}; }
    template<class U> void return_value(U&& value) {
        this->value.emplace(std::forward<U>(value));
    }
};

template<class Executor>
struct affine_task<void, Executor>::promise_type : detail::task_promise_base<void> {
    Executor executor;
    std::coroutine_handle<> continuation{};

    template<class... Args>
    explicit promise_type(Executor executor, Args&&...)
        : executor(std::move(executor)) {}

    affine_task get_return_object() noexcept {
        return affine_task{handle_type::from_promise(*this)};
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }

    struct final_awaiter {
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(handle_type handle) const noexcept {
            auto& promise = handle.promise();
            promise.state->complete(promise.state->error);
            if (!promise.continuation)
                return std::noop_coroutine();
            if (promise.executor.is_current_thread())
                return promise.continuation;
            if (!promise.executor.schedule(promise.continuation))
                std::terminate();
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    final_awaiter final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
};

template<class T, class Executor>
T sync_wait(affine_task<T, Executor> task) {
    auto state = task.state();
    if (!state)
        throw std::invalid_argument("sync_wait requires a valid affine_task");

    auto& executor = task.executor();
    task.start();
    executor.run_until([&] { return state->is_completed(); });

    if (state->error)
        std::rethrow_exception(state->error);

    // The public task API intentionally keeps ownership opaque; completion is
    // observed here and the task frame is released with the task object.
    return std::move(*task.handle_.promise().value);
}

template<class Executor>
void sync_wait(affine_task<void, Executor> task) {
    auto state = task.state();
    if (!state)
        throw std::invalid_argument("sync_wait requires a valid affine_task");
    auto& executor = task.executor();
    task.start();
    executor.run_until([&] { return state->is_completed(); });
    if (state->error)
        std::rethrow_exception(state->error);
}

template<class T, class Executor>
using main_actor_task = affine_task<T, Executor>;

} // namespace exec
