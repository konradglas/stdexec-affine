#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <affine/affine_task.hpp>
#include <affine/main_thread_executor.hpp>
#include <affine/root_scope.hpp>
#include <affine/serial_executor.hpp>

#if defined(AFFINE_TEST_HAS_STDEXEC)
#  include <affine/stdexec.hpp>
#  include <exec/single_thread_context.hpp>
#  include <exec/split.hpp>
#  include <exec/task.hpp>
#endif

#include <condition_variable>
#include <deque>
#include <memory>
#include <thread>

class host_queue {
public:
    void enqueue(std::function<void()> callback) {
        {
            std::lock_guard lock(_mutex);
            _queue.push_back(std::move(callback));
        }
        _condition.notify_one();
    }

    template<class Predicate>
    void pump_until(Predicate&& done) {
        while (!done()) {
            std::function<void()> callback;
            {
                std::unique_lock lock(_mutex);
                _condition.wait(lock, [&] { return !_queue.empty(); });
                callback = std::move(_queue.front());
                _queue.pop_front();
            }
            callback();
        }
    }

    bool empty() const {
        std::lock_guard lock(_mutex);
        return _queue.empty();
    }

private:
    mutable std::mutex _mutex;
    std::condition_variable _condition;
    std::deque<std::function<void()>> _queue;
};

struct worker_awaitable {
    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> continuation) const {
        std::thread([continuation] {
            continuation.resume();
        }).detach();
    }

    int await_resume() const noexcept { return 7; }
};

class manual_executor {
public:
    explicit manual_executor(std::thread::id owner = std::this_thread::get_id())
        : _state(std::make_shared<state>(owner)) {}

    bool is_current_thread() const noexcept {
        return std::this_thread::get_id() == _state->_owner;
    }

    bool schedule(std::coroutine_handle<> handle) {
        std::lock_guard lock(_state->_mutex);
        _state->_queue.push_back(handle);
        _state->_condition.notify_one();
        return true;
    }

    template<class Predicate>
    void run_until(Predicate&& done) {
        REQUIRE(is_current_thread());
        while (!done()) {
            std::coroutine_handle<> next;
            {
                std::unique_lock lock(_state->_mutex);
                _state->_condition.wait(lock, [&] {
                    return !_state->_queue.empty();
                });
                next = _state->_queue.front();
                _state->_queue.pop_front();
            }
            next.resume();
        }
    }

    void drain() {
        REQUIRE(is_current_thread());
        for (;;) {
            std::coroutine_handle<> next;
            {
                std::lock_guard lock(_state->_mutex);
                if (_state->_queue.empty())
                    return;
                next = _state->_queue.front();
                _state->_queue.pop_front();
            }
            next.resume();
        }
    }

private:
    struct state {
        explicit state(std::thread::id owner) : _owner(owner) {}
        std::thread::id _owner;
        std::mutex _mutex;
        std::condition_variable _condition;
        std::deque<std::coroutine_handle<>> _queue;
    };
    std::shared_ptr<state> _state;
};

struct inline_executor {
    bool is_current_thread() const noexcept { return true; }

    bool schedule(std::coroutine_handle<> handle) const {
        handle.resume();
        return true;
    }
};

affine::affine_task<int, manual_executor> answer(manual_executor executor) {
    co_return 42;
}

affine::affine_task<int, manual_executor> nested(manual_executor executor) {
    co_return co_await answer(executor);
}

affine::affine_task<int, manual_executor> default_executor_child() {
    co_return 42;
}

affine::affine_task<int, manual_executor> default_executor_parent() {
    co_return co_await default_executor_child();
}

template<class Executor>
affine::affine_task<void, Executor> mark_ran(Executor executor, bool& ran) {
    ran = true;
    co_return;
}

template<class Executor>
affine::affine_task<void, Executor> observe_stop(
    Executor executor, bool& observed) {
    const auto token = co_await affine::this_task::stop_token;
    observed = token.stop_requested();
    co_return;
}

template<class Executor>
affine::affine_task<void, Executor> observe_nested_stop(
    Executor executor, bool& observed) {
    co_await observe_stop(executor, observed);
    co_return;
}

affine::affine_task<void, manual_executor> observe_cross_executor_stop(
    manual_executor executor, bool& observed) {
    co_await observe_stop(inline_executor{}, observed);
}

affine::affine_task<bool, affine::serial_executor> observe_serial_executor(
    affine::serial_executor expected) {
    co_return expected.is_current_thread();
}

affine::affine_task<bool, affine::serial_executor> observe_inherited_serial_executor(
    affine::serial_executor expected) {
    const bool child_used_parent = co_await observe_serial_executor(expected);
    co_return child_used_parent && expected.is_current_thread();
}

affine::affine_task<void, affine::serial_executor> shutdown_scope(
    affine::root_scope& scope) {
    scope.shutdown_and_wait();
    co_return;
}

struct executor_switch_observation {
    bool child_used_second = false;
    bool parent_returned_to_first = false;
};

affine::affine_task<executor_switch_observation, affine::serial_executor>
observe_explicit_serial_executor_switch(affine::serial_executor first,
                                        affine::serial_executor second) {
    executor_switch_observation result;
    result.child_used_second =
        co_await observe_serial_executor(second).scheduleOn(second);
    result.parent_returned_to_first = first.is_current_thread();
    co_return result;
}

affine::affine_task<int, manual_executor> arbitrary_awaitable(
    manual_executor executor, bool& resumed_on_owner) {
    const auto value = co_await worker_awaitable{};
    resumed_on_owner = executor.is_current_thread();
    co_return value;
}

template<class Executor>
affine::affine_task<void, Executor> scoped_arbitrary_awaitable(
    Executor executor, bool& resumed_on_owner) {
    (void)co_await worker_awaitable{};
    resumed_on_owner = executor.is_current_thread();
}

TEST_CASE("sync_wait drives the affine executor on its owner thread") {
    manual_executor executor;
    CHECK(affine::sync_wait(answer(executor)) == 42);
}

TEST_CASE("nested tasks preserve executor affinity") {
    manual_executor executor;
    CHECK(affine::sync_wait(nested(executor)) == 42);
}

TEST_CASE("nested tasks inherit a default-constructed parent executor") {
    CHECK(affine::sync_wait(default_executor_parent()) == 42);
}

TEST_CASE("task body does not run before start") {
    manual_executor executor;
    bool ran = false;

    auto task = mark_ran(executor, ran);

    CHECK_FALSE(ran);
    affine::sync_wait(std::move(task));
    CHECK(ran);
}

TEST_CASE("tasks expose cooperative cancellation through their context") {
    manual_executor executor;
    bool observed = false;
    auto task = observe_stop(executor, observed);

    CHECK(task.request_stop());
    CHECK_FALSE(task.request_stop());
    affine::sync_wait(std::move(task));
    CHECK(observed);
}

TEST_CASE("nested tasks inherit cooperative cancellation") {
    manual_executor executor;
    bool observed = false;
    auto task = observe_nested_stop(executor, observed);

    CHECK(task.request_stop());
    affine::sync_wait(std::move(task));
    CHECK(observed);
}

TEST_CASE("tasks on another executor inherit cooperative cancellation") {
    manual_executor executor;
    bool observed = false;
    auto task = observe_cross_executor_stop(executor, observed);

    CHECK(task.request_stop());
    affine::sync_wait(std::move(task));
    CHECK(observed);
}

TEST_CASE("unbound child tasks inherit their parent executor") {
    affine::single_thread_context context;
    auto executor = context.get_executor();

    CHECK(affine::sync_wait(
        observe_inherited_serial_executor(executor).scheduleOn(executor)));

    context.shutdown_and_wait();
}

TEST_CASE("scheduleOn switches between instances of one executor type") {
    affine::single_thread_context first_context;
    affine::single_thread_context second_context;
    auto first = first_context.get_executor();
    auto second = second_context.get_executor();

    const auto observation = affine::sync_wait(
        observe_explicit_serial_executor_switch(first, second).scheduleOn(first));

    CHECK(observation.child_used_second);
    CHECK(observation.parent_returned_to_first);

    first_context.shutdown_and_wait();
    second_context.shutdown_and_wait();
}

TEST_CASE("spawn rejects an unbound root task") {
    affine::single_thread_context context;
    auto root = affine::create_root_scope();
    bool ran = false;

    CHECK_THROWS_AS(
        affine::spawn(mark_ran(context.get_executor(), ran), root),
        std::invalid_argument);

    root.shutdown_and_wait();
    context.shutdown_and_wait();
}

TEST_CASE("arbitrary awaitables hop the continuation back to the executor") {
    manual_executor executor;
    bool resumed_on_owner = false;

    CHECK(affine::sync_wait(arbitrary_awaitable(executor, resumed_on_owner)) == 7);
    CHECK(resumed_on_owner);
}

TEST_CASE("root scope owns an active task until drain completes") {
    manual_executor executor;
    bool resumed_on_owner = false;
    auto scope = affine::create_root_scope();

    affine::spawn(scoped_arbitrary_awaitable(executor, resumed_on_owner), scope);
    scope.close();
    scope.drain();

    CHECK(resumed_on_owner);
}

TEST_CASE("root shutdown waits for work on a serial executor") {
    affine::single_thread_context context;
    auto executor = context.get_executor();
    bool resumed_on_owner = false;
    auto scope = affine::create_root_scope();

    affine::spawn(
        scoped_arbitrary_awaitable(executor, resumed_on_owner)
            .scheduleOn(executor),
        scope);
    scope.shutdown_and_wait();

    CHECK(resumed_on_owner);
    context.shutdown_and_wait();
}

TEST_CASE("root shutdown drains from the serial executor thread") {
    affine::single_thread_context context;
    auto executor = context.get_executor();
    bool resumed_on_owner = false;
    auto scope = affine::create_root_scope();

    affine::spawn(
        scoped_arbitrary_awaitable(executor, resumed_on_owner)
            .scheduleOn(executor),
        scope);
    affine::sync_wait(shutdown_scope(scope).scheduleOn(executor));

    CHECK(resumed_on_owner);
    context.shutdown_and_wait();
}

TEST_CASE("child shutdown drains while the executor remains open") {
    host_queue host;
    affine::main_thread_executor executor(
        std::this_thread::get_id(),
        [&](std::function<void()> callback) { host.enqueue(std::move(callback)); });

    bool parent_resumed = false;
    bool child_resumed = false;
    auto root = affine::create_root_scope();
    auto parent = affine::spawn(
        scoped_arbitrary_awaitable(executor, parent_resumed)
            .scheduleOn(executor),
        root);
    auto child = affine::spawn(
        scoped_arbitrary_awaitable(executor, child_resumed)
            .scheduleOn(executor),
        parent);

    parent.shutdown_and_wait();

    CHECK(parent.completed());
    CHECK(child.completed());
    CHECK(parent_resumed);
    CHECK(child_resumed);
    CHECK(executor.accepting());

    root.shutdown_and_wait();
    host.pump_until([&] { return host.empty(); });
}

TEST_CASE("worker-thread waits block without draining the owner executor") {
    host_queue host;
    affine::main_thread_executor executor(
        std::this_thread::get_id(),
        [&](std::function<void()> callback) { host.enqueue(std::move(callback)); });

    bool resumed_on_owner = false;
    auto root = affine::create_root_scope();
    auto child = affine::spawn(
        scoped_arbitrary_awaitable(executor, resumed_on_owner)
            .scheduleOn(executor),
        root);

    std::atomic<bool> wait_returned = false;
    std::thread waiter([&] {
        child.wait();
        wait_returned.store(true, std::memory_order_release);
    });

    host.pump_until([&] { return child.completed(); });
    waiter.join();

    CHECK(wait_returned.load(std::memory_order_acquire));
    CHECK(resumed_on_owner);
    CHECK(executor.accepting());

    root.shutdown_and_wait();
}

TEST_CASE("main thread executor bridges a host dispatch queue") {
    host_queue host;
    affine::main_thread_executor executor(
        std::this_thread::get_id(),
        [&](std::function<void()> callback) { host.enqueue(std::move(callback)); },
        [&](std::function<bool()> done) { host.pump_until(std::move(done)); });

    bool ran = false;
    auto task = mark_ran(executor, ran).scheduleOn(executor);
    std::thread worker([&] { task.start(); });

    executor.run_until([&] { return task.done(); });
    worker.join();
    CHECK(ran);
}

TEST_CASE("closing main thread executor makes queued callbacks harmless") {
    host_queue host;
    affine::main_thread_executor executor(
        std::this_thread::get_id(),
        [&](std::function<void()> callback) { host.enqueue(std::move(callback)); });

    bool ran = false;
    auto task = mark_ran(executor, ran).scheduleOn(executor);
    std::thread worker([&] { task.start(); });
    worker.join();
    executor.close();
    executor.drain();

    host.pump_until([&] { return host.empty(); });
    CHECK(ran);
    CHECK(task.done());
    CHECK_FALSE(executor.accepting());
}

TEST_CASE("main thread executor drains claimed callbacks on its owner thread") {
    host_queue host;
    affine::main_thread_executor executor(
        std::this_thread::get_id(),
        [&](std::function<void()> callback) { host.enqueue(std::move(callback)); });

    bool ran = false;
    auto task = mark_ran(executor, ran).scheduleOn(executor);
    std::thread worker([&] { task.start(); });
    worker.join();

    executor.close();
    executor.drain();

    CHECK(ran);
    CHECK(task.done());
}

#if defined(AFFINE_TEST_HAS_STDEXEC)
affine::affine_task<int, manual_executor> sender_awaitable(
    manual_executor executor) {
    co_return co_await stdexec::just(99);
}

using single_thread_scheduler = decltype(
    std::declval<exec::single_thread_context&>().get_scheduler());

affine::affine_task<int, manual_executor> asynchronous_sender_awaitable(
    manual_executor executor,
    single_thread_scheduler scheduler,
    bool& resumed_on_owner) {
    const auto result = co_await stdexec::starts_on(
        std::move(scheduler), stdexec::just(101));
    resumed_on_owner = executor.is_current_thread();
    co_return result;
}

affine::affine_task<void, manual_executor> observe_stdexec_stop(
    manual_executor executor, bool& observed) {
    const auto token = co_await stdexec::read_env(stdexec::get_stop_token);
    observed = token.stop_requested();
    co_return;
}

affine::affine_task<void, manual_executor> stopped_sender(
    manual_executor executor) {
    co_return co_await stdexec::just_stopped();
}

exec::task<int> foreign_task() {
    co_await worker_awaitable{};
    co_return 42;
}

affine::affine_task<int, affine::main_thread_executor> scoped_child_task(
    affine::main_thread_executor executor,
    bool& resumed_on_owner) {
    const auto result = co_await foreign_task();
    resumed_on_owner = executor.is_current_thread();
    co_return result;
}

affine::affine_task<void, affine::main_thread_executor> scoped_root_task(
    affine::main_thread_executor executor,
    int& result,
    bool& resumed_on_owner) {
    result = co_await scoped_child_task(executor, resumed_on_owner);
}

TEST_CASE("stdexec senders are adapted as affine awaitables") {
    manual_executor executor;
    CHECK(affine::sync_wait(sender_awaitable(executor)) == 99);
}

// AGENTIC: split supplies a non-copyable operation-state reference as its stop callback.
affine::affine_task<int, manual_executor> shared_sender_awaitable(
    manual_executor executor, int& calls) {
    auto shared = exec::split(stdexec::just() | stdexec::then([&calls] {
        ++calls;
        return 42;
    }));
    const int first = co_await shared;
    const int second = co_await shared;
    co_return first + second;
}

TEST_CASE("split senders can be awaited repeatedly from affine tasks") {
    manual_executor executor;
    int calls = 0;
    CHECK(affine::sync_wait(shared_sender_awaitable(executor, calls)) == 84);
    CHECK(calls == 1);
}

TEST_CASE("stdexec stop adapter borrows non-copyable callbacks and owns value callbacks") {
    struct callback {
        int& calls;
        std::unique_ptr<int> owned = std::make_unique<int>(1);
        void operator()() noexcept { calls += *owned; }
    };
    std::stop_source source;
    affine::detail::stdexec_stop_token token(source.get_token());
    int borrowed_calls = 0, owned_calls = 0;
    callback borrowed{borrowed_calls};
    {
        affine::detail::stdexec_stop_token::callback_type<callback&> first(token, borrowed);
        affine::detail::stdexec_stop_token::callback_type<callback> second(token, callback{owned_calls});
        source.request_stop();
        CHECK(borrowed_calls == 1);
        CHECK(owned_calls == 1);
    }
    CHECK(borrowed.owned != nullptr);
}

TEST_CASE("asynchronous stdexec senders return to the affine executor") {
    exec::single_thread_context worker;
    manual_executor executor;
    bool resumed_on_owner = false;

    CHECK(affine::sync_wait(
        asynchronous_sender_awaitable(
            executor, worker.get_scheduler(), resumed_on_owner)
            .scheduleOn(executor)) == 101);
    CHECK(resumed_on_owner);
}

TEST_CASE("stdexec sender environment exposes the task stop token") {
    manual_executor executor;
    bool observed = false;
    auto task = observe_stdexec_stop(executor, observed);

    CHECK(task.request_stop());
    affine::sync_wait(std::move(task));
    CHECK(observed);
}

TEST_CASE("unwrapped stdexec stop becomes task cancellation") {
    manual_executor executor;
    CHECK_THROWS_AS(
        affine::sync_wait(stopped_sender(executor)),
        affine::operation_cancelled);
}

TEST_CASE("root scope drains a foreign task back onto the affine executor") {
    host_queue host;
    affine::main_thread_executor executor(
        std::this_thread::get_id(),
        [&](std::function<void()> callback) { host.enqueue(std::move(callback)); });

    int result = 0;
    bool resumed_on_owner = false;
    auto scope = affine::create_root_scope();
    affine::spawn(
        scoped_root_task(executor, result, resumed_on_owner)
            .scheduleOn(executor),
        scope);

    scope.close();
    scope.drain();

    CHECK(result == 42);
    CHECK(resumed_on_owner);
    CHECK_FALSE(executor.accepting());
    CHECK(host.empty());
}
#endif
