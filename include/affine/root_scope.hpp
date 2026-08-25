#pragma once

#include <affine/affine_task.hpp>

#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace affine {

namespace detail {

struct scoped_operation {
    virtual ~scoped_operation() = default;
    virtual void start() = 0;
    virtual void request_stop() noexcept = 0;
    virtual void close_executor() noexcept = 0;
    virtual void wait() = 0;
    [[nodiscard]] virtual bool completed() const noexcept = 0;
};

template<class Executor>
class scoped_task_operation final : public scoped_operation {
public:
    explicit scoped_task_operation(affine_task<void, Executor> task)
        : _task(std::move(task)) {}

    void start() override { _task.start(); }

    void request_stop() noexcept override { _task.request_stop(); }

    void close_executor() noexcept override {
        if constexpr (requires(Executor& executor) { executor.close(); }) {
            try {
                _task.executor().close();
            } catch (...) {
                std::terminate();
            }
        }
    }

    void wait() override {
        const auto state = _task.state();
        if (!state)
            return;

        if (!state->is_completed()) {
            auto& executor = _task.executor();
            if (executor.is_current_thread()) {
                if constexpr (drainable_executor<Executor>) {
                    executor.run_until([&] { return state->is_completed(); });
                } else {
                    throw std::logic_error(
                        "owner-thread waiting requires a drainable executor");
                }
            } else {
                state->wait();
            }
        }

        if (state->_error)
            std::rethrow_exception(state->_error);
    }

    [[nodiscard]] bool completed() const noexcept override {
        const auto state = _task.state();
        return !state || state->is_completed();
    }

private:
    affine_task<void, Executor> _task;
};

class task_control {
public:
    explicit task_control(std::shared_ptr<scoped_operation> operation = {})
        : _operation(std::move(operation)) {}

    ~task_control() {
        if (!completed())
            std::terminate();
    }

    void add_child(const std::shared_ptr<task_control>& child) {
        bool close_child = false;
        bool close_child_executor = false;
        {
            std::lock_guard lock(_mutex);
            if (_closing) {
                close_child = true;
                close_child_executor = _close_executors;
            }
            _children.push_back(child);
        }
        if (close_child)
            child->close(close_child_executor);
    }

    void remove_child(const std::shared_ptr<task_control>& child) noexcept {
        std::lock_guard lock(_mutex);
        std::erase(_children, child);
    }

    void close(bool close_executors) noexcept {
        std::shared_ptr<scoped_operation> operation;
        std::vector<std::shared_ptr<task_control>> children;
        {
            std::lock_guard lock(_mutex);
            _closing = true;
            _close_executors = _close_executors || close_executors;
            operation = _operation;
            children = _children;
        }

        if (operation) {
            operation->request_stop();
            if (close_executors)
                operation->close_executor();
        }
        for (const auto& child : children)
            child->close(close_executors);
    }

    void wait() {
        std::exception_ptr error;

        for (;;) {
            std::vector<std::shared_ptr<task_control>> children;
            {
                std::lock_guard lock(_mutex);
                children = _children;
            }
            if (children.empty())
                break;

            for (const auto& child : children) {
                try {
                    child->wait();
                } catch (...) {
                    if (!error)
                        error = std::current_exception();
                }
            }

            std::lock_guard lock(_mutex);
            std::erase_if(_children, [](const auto& child) {
                return child->completed();
            });
        }

        std::shared_ptr<scoped_operation> operation;
        {
            std::lock_guard lock(_mutex);
            operation = _operation;
        }
        if (operation) {
            try {
                operation->wait();
            } catch (...) {
                if (!error)
                    error = std::current_exception();
            }

            if (operation->completed()) {
                std::lock_guard lock(_mutex);
                if (_operation == operation)
                    _operation.reset();
            }
        }

        {
            std::lock_guard lock(_mutex);
            if (!_error)
                _error = error;
            else if (!error)
                error = _error;
        }

        if (error)
            std::rethrow_exception(error);
    }

    [[nodiscard]] bool completed() const noexcept {
        std::shared_ptr<scoped_operation> operation;
        std::vector<std::shared_ptr<task_control>> children;
        {
            std::lock_guard lock(_mutex);
            operation = _operation;
            children = _children;
        }
        if (operation && !operation->completed())
            return false;
        return std::ranges::all_of(children, [](const auto& child) {
            return child->completed();
        });
    }

private:
    mutable std::mutex _mutex;
    bool _closing = false;
    bool _close_executors = false;
    std::exception_ptr _error;
    std::shared_ptr<scoped_operation> _operation;
    std::vector<std::shared_ptr<task_control>> _children;
};

} // namespace detail

class task_handle {
public:
    task_handle() noexcept = default;

    [[nodiscard]] explicit operator bool() const noexcept {
        return static_cast<bool>(_control);
    }

    void close() const noexcept {
        if (_control)
            _control->close(false);
    }

    void wait() const {
        if (_control)
            _control->wait();
    }

    void shutdown_and_wait() const {
        close();
        wait();
    }

    [[nodiscard]] bool completed() const noexcept {
        return !_control || _control->completed();
    }

private:
    explicit task_handle(std::shared_ptr<detail::task_control> control)
        : _control(std::move(control)) {}

    template<class Executor>
    friend task_handle spawn(affine_task<void, Executor>, const task_handle&);
    template<class Executor>
    friend task_handle spawn(affine_task<void, Executor>, const class root_scope&);
    template<class Executor, class Parent>
    friend task_handle spawn_impl(affine_task<void, Executor>, const Parent&);

    std::shared_ptr<detail::task_control> _control;
};

class root_scope {
public:
    root_scope() : _control(std::make_shared<detail::task_control>()) {}

    void close() const noexcept { _control->close(true); }
    void drain() const { _control->wait(); }

    void shutdown_and_wait() const {
        close();
        drain();
    }

    [[nodiscard]] bool completed() const noexcept {
        return _control->completed();
    }

private:
    template<class Executor>
    friend task_handle spawn(affine_task<void, Executor>, const root_scope&);
    template<class Executor, class Parent>
    friend task_handle spawn_impl(affine_task<void, Executor>, const Parent&);

    std::shared_ptr<detail::task_control> _control;
};

inline root_scope create_root_scope() { return {}; }

template<class Executor, class Parent>
task_handle spawn_impl(affine_task<void, Executor> task, const Parent& parent) {
    if (!task.valid())
        throw std::invalid_argument("spawn requires a valid affine_task");

    auto operation =
        std::make_shared<detail::scoped_task_operation<Executor>>(std::move(task));
    auto control = std::make_shared<detail::task_control>(operation);
    parent._control->add_child(control);
    try {
        operation->start();
    } catch (...) {
        parent._control->remove_child(control);
        throw;
    }
    return task_handle{std::move(control)};
}

template<class Executor>
task_handle spawn(affine_task<void, Executor> task, const root_scope& parent) {
    return spawn_impl(std::move(task), parent);
}

template<class Executor>
task_handle spawn(affine_task<void, Executor> task, const task_handle& parent) {
    if (!parent)
        throw std::invalid_argument("spawn requires a valid parent task_handle");
    return spawn_impl(std::move(task), parent);
}

} // namespace affine
