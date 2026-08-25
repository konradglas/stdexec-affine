#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <affine/affine_task.hpp>

#if defined(AFFINE_TEST_HAS_STDEXEC)
#  include <affine/stdexec.hpp>
#endif

#include <condition_variable>
#include <deque>
#include <memory>
#include <thread>

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

affine::affine_task<void, manual_executor> mark_ran(manual_executor executor, bool& ran) {
    ran = true;
    co_return;
}

affine::affine_task<int, manual_executor> arbitrary_awaitable(
    manual_executor executor, bool& resumed_on_owner) {
    const auto value = co_await worker_awaitable{};
    resumed_on_owner = executor.is_current_thread();
    co_return value;
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

TEST_CASE("arbitrary awaitables hop the continuation back to the executor") {
    manual_executor executor;
    bool resumed_on_owner = false;

    CHECK(affine::sync_wait(arbitrary_awaitable(executor, resumed_on_owner)) == 7);
    CHECK(resumed_on_owner);
}

#if defined(AFFINE_TEST_HAS_STDEXEC)
affine::affine_task<int, manual_executor> sender_awaitable(
    manual_executor executor) {
    co_return co_await stdexec::just(99);
}

TEST_CASE("stdexec senders are adapted as affine awaitables") {
    manual_executor executor;
    CHECK(affine::sync_wait(sender_awaitable(executor)) == 99);
}
#endif
