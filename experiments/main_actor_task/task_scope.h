#pragma once

#include "main_actor_task.h"
#include "main_thread_scheduler.h"

#include <stdexec/execution.hpp>

#include <atomic>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <utility>

namespace playground {

// root_scope / task_handle for MainActorTask, after include/affine/root_scope.hpp:
// closing a handle requests stop without waiting; the root's shutdown_and_wait is the
// only wait, and it drains the main loop itself instead of pumping the host.

class root_scope;
class task_handle;
task_handle spawn(MainActorTask<void> task, root_scope& scope);

// One spawned task: close() requests stop and returns; completed() says it has finished.
class task_handle {
public:
    task_handle() = default;

    void close() const noexcept {
        if (_state)
            _state->stop.request_stop();
    }

    [[nodiscard]] bool completed() const noexcept {
        return !_state || _state->completed.load(std::memory_order_acquire);
    }

private:
    friend task_handle spawn(MainActorTask<void> task, root_scope& scope);

    struct ScopeStop {
        stdexec::inplace_stop_source* source;
        void operator()() const noexcept { source->request_stop(); }
    };

    struct State {
        stdexec::inplace_stop_source stop;
        std::atomic<bool> completed{false};
    };

    // Owned by the spawned operation, so it ends with it whether the operation ran or was
    // dropped unstarted by a joined scope: links the root's stop to this task's, then marks
    // the task completed.
    struct Lifetime {
        Lifetime(std::shared_ptr<State> taskState, stdexec::inplace_stop_token rootToken)
        : state(std::move(taskState)), rootLink(rootToken, ScopeStop{&state->stop}) {}
        ~Lifetime() { state->completed.store(true, std::memory_order_release); }

        std::shared_ptr<State> state;
        stdexec::inplace_stop_callback<ScopeStop> rootLink;
    };

    explicit task_handle(std::shared_ptr<State> state) : _state(std::move(state)) {}

    std::shared_ptr<State> _state;
};

// Owns the main loop and every task spawned into it. Construct, shut down and destroy it on
// the owner (main) thread; spawn may be called from any thread.
class root_scope {
public:
    using ErrorHandler = std::function<void(std::exception_ptr)>;

    explicit root_scope(Wakeup wakeup, ErrorHandler onError = logError)
    : _loop(std::move(wakeup)), _onError(std::move(onError)) {}

    root_scope(const root_scope&) = delete;
    root_scope& operator=(const root_scope&) = delete;

    ~root_scope() { shutdown_and_wait(); }

    // Requests stop for every task, then joins them all, draining the main loop on this thread.
    void shutdown_and_wait() {
        if (std::exchange(_joined, true))
            return;
        _stop.request_stop();
        _loop.sync_wait(_scope.join());
        _loop.finish();
    }

    [[nodiscard]] MainThreadLoop::Scheduler scheduler() const noexcept { return _loop.get_scheduler(); }

private:
    friend task_handle spawn(MainActorTask<void> task, root_scope& scope);

    static void logError(std::exception_ptr error) {
        try {
            std::rethrow_exception(error);
        } catch (const std::exception& exception) {
            std::cerr << "MainActorTask failed: " << exception.what() << '\n';
        } catch (...) {
            std::cerr << "MainActorTask failed with an unknown exception\n";
        }
    }

    void report(std::exception_ptr error) noexcept {
        try {
            _onError(std::move(error));
        } catch (...) {
        }
    }

    MainThreadLoop _loop;
    ErrorHandler _onError;
    stdexec::inplace_stop_source _stop;
    stdexec::simple_counting_scope _scope;
    bool _joined = false;
};

// Starts the task on the scope's main loop. An exception escaping it goes to the scope's
// error handler. After shutdown the task is dropped unstarted and its handle is completed.
inline task_handle spawn(MainActorTask<void> task, root_scope& scope) {
    auto state = std::make_shared<task_handle::State>();
    if (scope._stop.stop_requested()) {
        state->completed.store(true, std::memory_order_release);
        return task_handle{std::move(state)};
    }
    auto lifetime = std::make_unique<task_handle::Lifetime>(state, scope._stop.get_token());

    auto operation = stdexec::starts_on(scope._loop.get_scheduler(),
                                        stdexec::write_env(std::move(task), stdexec::prop{stdexec::get_stop_token, state->stop.get_token()}))
                   | stdexec::upon_error([&scope](std::exception_ptr error) noexcept { scope.report(std::move(error)); })
                   | stdexec::upon_stopped([]() noexcept {})
                   | stdexec::then([lifetime = std::move(lifetime)]() noexcept {});
    stdexec::spawn(std::move(operation), scope._scope.get_token());
    return task_handle{std::move(state)};
}

} // namespace playground
