#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <utility>

namespace playground {

// Simulates dispatch_async(main) or a Windows message queue for the console host.
class TestMainQueue {
public:
    void post(std::function<void()> callback) {
        {
            std::lock_guard lock(mutex);
            callbacks.push_back(std::move(callback));
        }
        condition.notify_one();
    }

    void dispatchOne() {
        std::function<void()> callback;
        {
            std::unique_lock lock(mutex);
            condition.wait(lock, [this] { return !callbacks.empty(); });
            callback = std::move(callbacks.front());
            callbacks.pop_front();
        }
        callback();
    }

    void drainPosts() {
        for (;;) {
            std::function<void()> callback;
            {
                std::lock_guard lock(mutex);
                if (callbacks.empty())
                    return;
                callback = std::move(callbacks.front());
                callbacks.pop_front();
            }
            callback();
        }
    }

private:
    std::mutex mutex;
    std::condition_variable condition;
    std::deque<std::function<void()>> callbacks;
};

} // namespace playground
