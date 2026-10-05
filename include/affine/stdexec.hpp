#pragma once

#include <affine/affine_task.hpp>

#include <exec/task.hpp>
#include <stdexec/execution.hpp>

#include <memory>
#include <optional>
#include <utility>

namespace affine::detail {

class stdexec_stop_token {
public:
    template<class Callback>
    class callback_type {
        // AGENTIC: split registers a non-copyable operation by reference.
        // Defer invocation through a wrapper, preserving references and owning values.
        struct callback_wrapper {
            Callback callback;
            void operator()() noexcept { callback(); }
        };

    public:
        callback_type(stdexec_stop_token token, Callback callback) noexcept
        : _callback(token._token, callback_wrapper{ std::forward<Callback>(callback) }) {}

        template<class StopSource>
        callback_type(stdexec_stop_token token, StopSource& source) noexcept
        : callback_type(token, Callback{ source }) {}

    private:
        std::stop_callback<callback_wrapper> _callback;
    };

    stdexec_stop_token() noexcept = default;
    explicit stdexec_stop_token(std::stop_token token) noexcept
    : _token(std::move(token)) {}

    [[nodiscard]] bool stop_requested() const noexcept { return _token.stop_requested(); }

    [[nodiscard]] bool stop_possible() const noexcept { return _token.stop_possible(); }

    friend bool operator==(const stdexec_stop_token&, const stdexec_stop_token&) = default;

private:
    std::stop_token _token;
};

template<class Executor>
struct executor_hop {
    Executor _executor;

    bool await_ready() const noexcept { return _executor.is_current_thread(); }

    void await_suspend(std::coroutine_handle<> continuation) {
        if (!_executor.schedule(continuation))
            throw std::runtime_error("affine executor rejected continuation");
    }

    void await_resume() const noexcept {}
};

template<class T, class Context, class Executor>
auto continue_exec_task_on_executor(exec::basic_task<T, Context> task, Executor executor) -> exec::task<T> {
    std::exception_ptr error;

    if constexpr (std::is_void_v<T>) {
        try {
            co_await stdexec::stopped_as_error(std::move(task), std::make_exception_ptr(operation_cancelled{}));
        } catch (...) { error = std::current_exception(); }
        co_await executor_hop<Executor>{ std::move(executor) };
        if (error)
            std::rethrow_exception(error);
        co_return;
    } else if constexpr (std::is_lvalue_reference_v<T>) {
        using value_type = std::remove_reference_t<T>;
        value_type* result = nullptr;
        try {
            result = std::addressof(co_await stdexec::stopped_as_error(std::move(task), std::make_exception_ptr(operation_cancelled{})));
        } catch (...) { error = std::current_exception(); }
        co_await executor_hop<Executor>{ std::move(executor) };
        if (error)
            std::rethrow_exception(error);
        co_return static_cast<T>(*result);
    } else {
        std::optional<std::remove_cv_t<T>> result;
        try {
            result.emplace(co_await stdexec::stopped_as_error(std::move(task), std::make_exception_ptr(operation_cancelled{})));
        } catch (...) { error = std::current_exception(); }
        co_await executor_hop<Executor>{ std::move(executor) };
        if (error)
            std::rethrow_exception(error);
        co_return std::move(*result);
    }
}

template<class T, class Sender>
    requires(!std::is_void_v<T>)
auto sender_as_exec_task(Sender sender) -> exec::task<T> {
    co_return co_await std::move(sender);
}

template<class T, class Sender>
    requires std::is_void_v<T>
auto sender_as_exec_task(Sender sender) -> exec::task<void> {
    co_await std::move(sender);
}

template<class Promise>
struct environment_adapter<Promise, std::void_t<decltype(std::declval<Promise const&>().stop_token())>> {
    static auto make(Promise const& promise) noexcept {
        return stdexec::env{ stdexec::prop{ stdexec::get_stop_token, stdexec_stop_token{ promise._state->_stop_source.get_token() } },
                             stdexec::prop{ stdexec::get_scheduler, stdexec::inline_scheduler{} },
                             stdexec::prop{ stdexec::get_start_scheduler, stdexec::inline_scheduler{} } };
    }
};

template<class Sender, class Promise>
    requires stdexec::sender<Sender>
struct awaitable_adapter<Sender, Promise> {
    static constexpr bool customized = true;
    static constexpr bool preserves_affinity = true;

    template<class S, class Executor>
    static auto adapt(S&& sender, Promise& promise, Executor executor) {
        using sender_awaiter = decltype(stdexec::as_awaitable(std::declval<S>(), promise));
        using result_type = decltype(std::declval<sender_awaiter&>().await_resume());
        auto sender_with_environment = stdexec::write_env(static_cast<S&&>(sender), promise.get_env());
        return stdexec::as_awaitable(continue_exec_task_on_executor(sender_as_exec_task<result_type>(std::move(sender_with_environment)), std::move(executor)),
                                     promise);
    }
};

template<class T, class Context, class Promise>
struct awaitable_adapter<exec::basic_task<T, Context>, Promise> {
    static constexpr bool customized = true;
    static constexpr bool preserves_affinity = true;

    template<class Task, class Executor>
    static auto adapt(Task&& task, Promise& promise, Executor executor) {
        return stdexec::as_awaitable(continue_exec_task_on_executor(static_cast<Task&&>(task), std::move(executor)), promise);
    }
};

}  // namespace affine::detail
