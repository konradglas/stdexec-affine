#include "task_scope.h"
#include "test_main_queue.h"

#include <exec/single_thread_context.hpp>
#include <exec/timed_thread_scheduler.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <latch>
#include <optional>
#include <stdexcept>
#include <thread>

using namespace playground;

namespace {

int failures = 0;

void check(bool condition, const char* what) {
    std::cout << (condition ? "ok:   " : "FAIL: ") << what << std::endl;
    if (!condition)
        ++failures;
}

std::thread::id mainThread;
bool onMain() { return std::this_thread::get_id() == mainThread; }

struct Host {
    TestMainQueue queue;
    Wakeup wakeup() {
        return [this](std::function<void()> callback) { queue.post(std::move(callback)); };
    }
    template<class Predicate>
    void pumpUntil(Predicate done) {
        while (!done())
            queue.dispatchOne();
    }
};

struct SetOnDestroy {
    bool& flag;
    ~SetOnDestroy() { flag = true; }
};

MainActorTask<int> nested() {
    co_return onMain() ? 7 : -1;
}

MainActorTask<void> roundTrip(exec::single_thread_context& worker, bool& done) {
    bool ok = onMain();
    auto workerThread = co_await (stdexec::schedule(worker.get_scheduler()) | stdexec::then([] { return std::this_thread::get_id(); }));
    ok = ok && workerThread != mainThread && onMain();
    ok = ok && co_await nested() == 7;
    try {
        co_await (stdexec::schedule(worker.get_scheduler()) | stdexec::then([] { throw std::runtime_error("worker failure"); }));
        ok = false;
    } catch (const std::runtime_error&) {
        ok = ok && onMain();
    }
    done = ok;
}

MainActorTask<void> waitForever(exec::timed_thread_context& timer, bool& resumed, bool& destroyed) {
    SetOnDestroy guard{destroyed};
    co_await exec::schedule_after(timer.get_scheduler(), std::chrono::hours(1));
    resumed = true;
}

MainActorTask<void> waitForRelease(exec::single_thread_context& worker, std::atomic<bool>& release, bool& resumed, bool& destroyed) {
    SetOnDestroy guard{destroyed};
    co_await (stdexec::schedule(worker.get_scheduler()) | stdexec::then([&release] {
                  while (!release.load())
                      std::this_thread::yield();
              }));
    // Stop was requested while the worker ran: this await completes stopped and unwinds.
    co_await stdexec::schedule(worker.get_scheduler());
    resumed = true;
}

MainActorTask<void> failAfterHop(exec::single_thread_context& worker) {
    co_await stdexec::schedule(worker.get_scheduler());
    throw std::runtime_error("escaped");
}

struct Hop {
    std::latch started{1};
    std::latch release{1};
    std::thread::id resumed;
    bool stopped = false;
    bool completed = false;
};

// Blocks a worker until released, then records where it resumed and whether it was stopped.
MainActorTask<void> hopAndRecord(exec::single_thread_context& worker, Hop& hop) {
    co_await (stdexec::schedule(worker.get_scheduler()) | stdexec::then([&hop] {
                  hop.started.count_down();
                  hop.release.wait();
              }));
    hop.resumed = std::this_thread::get_id();
    auto token = co_await stdexec::read_env(stdexec::get_stop_token);
    if (token.stop_requested()) {
        hop.stopped = true;
        co_return;
    }
    hop.completed = true;
}

MainActorTask<void> hops(exec::single_thread_context& worker, int count, int& completed) {
    for (int hop = 0; hop < count; ++hop) {
        co_await stdexec::schedule(worker.get_scheduler());
        ++completed;
    }
}

// Completes only stopped, from its stop callback: on main that resumes the task inline.
struct StopOnly {
    using sender_concept = stdexec::sender_t;
    using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t(), stdexec::set_stopped_t()>;

    template<class Receiver>
    struct Operation {
        struct OnStop {
            Operation* self;
            void operator()() const noexcept { stdexec::set_stopped(std::move(self->receiver)); }
        };
        using Token = stdexec::stop_token_of_t<stdexec::env_of_t<Receiver>>;

        Receiver receiver;
        std::optional<stdexec::stop_callback_for_t<Token, OnStop>> onStop;

        void start() noexcept { onStop.emplace(stdexec::get_stop_token(stdexec::get_env(receiver)), OnStop{this}); }
    };

    template<class Receiver>
    auto connect(Receiver receiver) const -> Operation<Receiver> {
        return {std::move(receiver), std::nullopt};
    }
};

MainActorTask<void> waitForStop(bool& resumed, bool& destroyed) {
    SetOnDestroy guard{destroyed};
    co_await StopOnly{};
    resumed = true;
}

} // namespace

int main() {
    mainThread = std::this_thread::get_id();
    exec::single_thread_context worker;
    exec::timed_thread_context timer;

    {
        Host host;
        root_scope scope(host.wakeup());
        bool done = false;
        auto handle = spawn(roundTrip(worker, done), scope);
        host.pumpUntil([&] { return handle.completed(); });
        check(done, "Body runs on main, hops to a worker, resumes on main, awaits a nested MainActorTask, catches a worker exception on main");
        scope.shutdown_and_wait();
        host.queue.drainPosts();
    }
    {
        Host host;
        root_scope scope(host.wakeup());
        bool closedResumed = false, closedDestroyed = false, otherResumed = false, otherDestroyed = false;
        auto closed = spawn(waitForever(timer, closedResumed, closedDestroyed), scope);
        auto other = spawn(waitForever(timer, otherResumed, otherDestroyed), scope);
        closed.close();
        host.pumpUntil([&] { return closed.completed(); });
        check(closedDestroyed && !closedResumed, "close() unwinds that task without resuming it, without waiting");
        check(!other.completed(), "close() leaves the other tasks running");
        scope.shutdown_and_wait();
        check(other.completed() && otherDestroyed && !otherResumed, "shutdown_and_wait stops and joins the rest");
        host.queue.drainPosts();
    }
    {
        Host host;
        root_scope scope(host.wakeup());
        std::atomic<bool> release{false};
        bool resumed = false, destroyed = false;
        auto handle = spawn(waitForRelease(worker, release, resumed, destroyed), scope);
        // Spawned on the owner thread, the task has already started inline and is on the worker.
        std::thread releaser([&release] {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            release.store(true);
        });
        scope.shutdown_and_wait();  // the host queue is never pumped from here on
        releaser.join();
        check(handle.completed() && destroyed && !resumed, "shutdown_and_wait joins a task blocked on a worker without the host pumping");
        host.queue.drainPosts();
    }
    {
        Host host;
        int reported = 0;
        root_scope scope(host.wakeup(), [&reported](std::exception_ptr) { ++reported; });
        auto handle = spawn(failAfterHop(worker), scope);
        host.pumpUntil([&] { return handle.completed(); });
        check(reported == 1, "An exception escaping a spawned task reaches the error handler");
        scope.shutdown_and_wait();
        host.queue.drainPosts();
    }
    {
        Host host;
        root_scope scope(host.wakeup());
        scope.shutdown_and_wait();
        bool done = false;
        auto handle = spawn(roundTrip(worker, done), scope);
        check(handle.completed() && !done, "A task spawned after shutdown never starts and is completed");
        host.queue.drainPosts();
    }

    {
        // Waiting on A runs A and its child A1 on the joining thread, never sibling B; the
        // root's wait then stops and joins B without B being waited first.
        Host host;
        root_scope scope(host.wakeup());
        exec::single_thread_context workerA, workerA1, workerB;
        Hop a, a1, b;
        auto handleA = spawn(hopAndRecord(workerA, a), scope);
        auto handleA1 = spawn(hopAndRecord(workerA1, a1), handleA);
        auto handleB = spawn(hopAndRecord(workerB, b), scope);
        a.started.wait();
        a1.started.wait();
        b.started.wait();
        handleA.close();
        a.release.count_down();
        a1.release.count_down();
        b.release.count_down();
        // All three continuations are queued before anyone joins.
        stdexec::sync_wait(stdexec::schedule(workerA.get_scheduler()));
        stdexec::sync_wait(stdexec::schedule(workerA1.get_scheduler()));
        stdexec::sync_wait(stdexec::schedule(workerB.get_scheduler()));
        std::thread::id joining;
        std::thread joiner([&] {
            joining = std::this_thread::get_id();
            handleA.wait(true);
        });
        joiner.join();
        check(a.stopped && a1.stopped, "close() on a handle reaches its children");
        check(a.resumed == joining && a1.resumed == joining && handleA.completed(),
              "wait() on a handle joins and pumps its children");
        check(b.resumed == std::thread::id{} && !handleB.completed(), "wait() on a handle never pumps a sibling");
        scope.shutdown_and_wait();
        check(b.stopped && b.resumed == mainThread && handleB.completed(),
              "shutdown_and_wait stops, pumps and joins the rest without waiting handles first");
        host.queue.drainPosts();
    }
    {
        // A stop requested on main completes the waiting task inline and frees it while
        // the stop request is still running.
        Host host;
        root_scope scope(host.wakeup());
        bool resumed = false, destroyed = false;
        auto handle = spawn(waitForStop(resumed, destroyed), scope);
        handle.close();
        check(destroyed && !resumed && handle.completed(), "A stop on main may complete and free the task inline");
        scope.shutdown_and_wait();
        host.queue.drainPosts();
    }
    {
        // The owner waits while each hop's continuation is queued from the worker and run.
        Host host;
        root_scope scope(host.wakeup());
        constexpr int rounds = 100;
        constexpr int count = 100;
        int completed = 0;
        for (int round = 0; round < rounds; ++round)
            spawn(hops(worker, count, completed), scope).wait();
        check(completed == rounds * count, "The owner's wait runs every hop of the awaited task");
        scope.shutdown_and_wait();
        host.queue.drainPosts();
    }
    {
        // A joining thread may drop the last handle as soon as the task completes.
        Host host;
        root_scope scope(host.wakeup());
        constexpr int rounds = 1000;
        int completed = 0;
        for (int round = 0; round < rounds; ++round) {
            auto handle = spawn(hops(worker, 1, completed), scope);
            std::thread joiner([handle = std::move(handle)]() mutable {
                handle.wait(true);
                handle = {};
            });
            joiner.join();
        }
        check(completed == rounds, "Joining threads run and retire every task");
        scope.shutdown_and_wait();
        host.queue.drainPosts();
    }

    std::cout << (failures == 0 ? "All task scope tests passed.\n" : "Task scope tests FAILED.\n");
    return failures == 0 ? 0 : 1;
}
