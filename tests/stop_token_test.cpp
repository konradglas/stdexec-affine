#include <doctest/doctest.h>
#include <affine/stop_token.hpp>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

TEST_CASE("stop tokens retain shared cancellation and source lifetime") {
    affine::stop_token token;
    CHECK_FALSE(token.stop_possible());
    {
        affine::stop_source source;
        token = source.get_token();
        auto copy = source;
        CHECK(token == copy.get_token());
        CHECK(token.stop_possible());
        CHECK_FALSE(token.stop_requested());
    }
    CHECK_FALSE(token.stop_possible());

    affine::stop_source source;
    token = source.get_token();
    int calls = 0;
    affine::stop_callback callback(token, [&] { ++calls; });
    CHECK(source.request_stop());
    CHECK_FALSE(source.request_stop());
    affine::stop_callback late(token, [&] { ++calls; });
    CHECK(calls == 2);
    CHECK(token.stop_requested());
}

TEST_CASE("stop callbacks can unregister and destroy themselves during cancellation") {
    affine::stop_source source;
    int calls = 0;
    {
        affine::stop_callback removed(source.get_token(), [&] { ++calls; });
    }
    using Callback = affine::stop_callback<std::function<void()>>;
    std::unique_ptr<Callback> callback;
    callback = std::make_unique<Callback>(source.get_token(), [&] {
        ++calls;
        callback.reset();
    });
    CHECK(source.request_stop());
    CHECK(calls == 1);
    CHECK_FALSE(callback);
}

TEST_CASE("stop callbacks may destroy the requesting source and cancel recursively") {
    auto source = std::make_unique<affine::stop_source>();
    auto token = source->get_token();
    int calls = 0;
    affine::stop_callback callback(token, [&] {
        CHECK_FALSE(source->request_stop());
        source.reset();
        ++calls;
    });
    CHECK(source->request_stop());
    CHECK(calls == 1);
    CHECK(token.stop_requested());
    CHECK(token.stop_possible());
}

TEST_CASE("callback destruction waits for an invocation on another thread") {
    affine::stop_source source;
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false;
    bool release = false;
    std::atomic<bool> destroying{false};
    std::atomic<bool> destroyed{false};
    using Callback = affine::stop_callback<std::function<void()>>;
    auto callback = std::make_unique<Callback>(source.get_token(), [&] {
        std::unique_lock lock(mutex);
        entered = true;
        changed.notify_all();
        changed.wait(lock, [&] { return release; });
    });
    std::thread requester([&] { source.request_stop(); });
    {
        std::unique_lock lock(mutex);
        changed.wait(lock, [&] { return entered; });
    }
    std::thread destroyer([&] {
        destroying = true;
        callback.reset();
        destroyed = true;
    });
    while (!destroying.load()) {
        std::this_thread::yield();
    }
    CHECK_FALSE(destroyed.load());
    {
        std::lock_guard lock(mutex);
        release = true;
    }
    changed.notify_all();
    requester.join();
    destroyer.join();
    CHECK(destroyed.load());
}

TEST_CASE("registration and removal can race cancellation") {
    for (int iteration = 0; iteration < 200; ++iteration) {
        affine::stop_source source;
        std::atomic<bool> go{false};
        std::atomic<int> calls{0};
        std::thread requester([&] {
            while (!go.load()) {
                std::this_thread::yield();
            }
            source.request_stop();
        });
        go = true;
        {
            affine::stop_callback callback(source.get_token(), [&] { ++calls; });
        }
        requester.join();
        CHECK(calls.load() <= 1);
        affine::stop_callback late(source.get_token(), [value = std::make_unique<int>(1), &calls] {
            calls += *value;
        });
        CHECK(calls.load() >= 1);
    }
}
