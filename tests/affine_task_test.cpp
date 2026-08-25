#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <exec/affine_task.hpp>

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
        : state_(std::make_shared<state>(owner)) {}

    bool is_current_thread() const noexcept {
        return std::this_thread::get_id() == state_->owner;
    }

    bool schedule(std::coroutine_handle<> handle) {
        std::lock_guard lock(state_->mutex);
        state_->queue.push_back(handle);
        state_->condition.notify_one();
        return true;
    }

    template<class Predicate>
    void run_until(Predicate&& done) {
        REQUIRE(is_current_thread());
        while (!done()) {
            std::coroutine_handle<> next;
            {
                std::unique_lock lock(state_->mutex);
                state_->condition.wait(lock, [&] {
                    return !state_->queue.empty();
                });
                next = state_->queue.front();
                state_->queue.pop_front();
            }
            next.resume();
        }
    }

private:
    struct state {
        explicit state(std::thread::id owner) : owner(owner) {}
        std::thread::id owner;
        std::mutex mutex;
        std::condition_variable condition;
        std::deque<std::coroutine_handle<>> queue;
    };
    std::shared_ptr<state> state_;
};

exec::affine_task<int, manual_executor> answer(manual_executor executor) {
    co_return 42;
}

exec::affine_task<int, manual_executor> nested(manual_executor executor) {
    co_return co_await answer(executor);
}

exec::affine_task<void, manual_executor> mark_ran(manual_executor executor, bool& ran) {
    ran = true;
    co_return;
}

exec::affine_task<int, manual_executor> arbitrary_awaitable(
    manual_executor executor, bool& resumed_on_owner) {
    const auto value = co_await worker_awaitable{};
    resumed_on_owner = executor.is_current_thread();
    co_return value;
}

TEST_CASE("sync_wait drives the affine executor on its owner thread") {
    manual_executor executor;
    CHECK(exec::sync_wait(answer(executor)) == 42);
}

TEST_CASE("nested tasks preserve executor affinity") {
    manual_executor executor;
    CHECK(exec::sync_wait(nested(executor)) == 42);
}

TEST_CASE("task body does not run before start") {
    manual_executor executor;
    bool ran = false;

    auto task = mark_ran(executor, ran);

    CHECK_FALSE(ran);
    exec::sync_wait(std::move(task));
    CHECK(ran);
}

TEST_CASE("arbitrary awaitables hop the continuation back to the executor") {
    manual_executor executor;
    bool resumed_on_owner = false;

    CHECK(exec::sync_wait(arbitrary_awaitable(executor, resumed_on_owner)) == 7);
    CHECK(resumed_on_owner);
}
