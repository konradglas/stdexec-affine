#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <affine/affine_task.hpp>
#include <affine/main_thread_executor.hpp>
#include <affine/root_scope.hpp>

#if defined(AFFINE_TEST_HAS_STDEXEC)
#  include <affine/stdexec.hpp>
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

affine::affine_task<int, manual_executor> answer(manual_executor executor) {
    co_return 42;
}

affine::affine_task<int, manual_executor> nested(manual_executor executor) {
    co_return co_await answer(executor);
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

affine::affine_task<int, manual_executor> arbitrary_awaitable(
    manual_executor executor, bool& resumed_on_owner) {
    const auto value = co_await worker_awaitable{};
    resumed_on_owner = executor.is_current_thread();
    co_return value;
}

affine::affine_task<void, manual_executor> scoped_arbitrary_awaitable(
    manual_executor executor, bool& resumed_on_owner) {
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

TEST_CASE("main thread executor bridges a host dispatch queue") {
    host_queue host;
    affine::main_thread_executor executor(
        std::this_thread::get_id(),
        [&](std::function<void()> callback) { host.enqueue(std::move(callback)); },
        [&](std::function<bool()> done) { host.pump_until(std::move(done)); });

    bool ran = false;
    auto task = mark_ran(executor, ran);
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
    auto task = mark_ran(executor, ran);
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
    auto task = mark_ran(executor, ran);
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
    affine::spawn(scoped_root_task(executor, result, resumed_on_owner), scope);

    scope.close();
    scope.drain();

    CHECK(result == 42);
    CHECK(resumed_on_owner);
    CHECK_FALSE(executor.accepting());
    CHECK(host.empty());
}
#endif
