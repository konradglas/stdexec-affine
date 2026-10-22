#include "task_scope.h"
#include "test_main_queue.h"

#include <exec/single_thread_context.hpp>
#include <exec/timed_thread_scheduler.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
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

    std::cout << (failures == 0 ? "All task scope tests passed.\n" : "Task scope tests FAILED.\n");
    return failures == 0 ? 0 : 1;
}
