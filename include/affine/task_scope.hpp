#pragma once

#include "affine/main_actor_task.hpp"
#include "affine/main_thread_scheduler.hpp"

#include <stdexec/execution.hpp>

#include <atomic>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace affine {

// root_scope / child_scope / task_handle for MainActorTask, as a tree:
// every spawned task gets its own queue (a child domain of its parent's) and scope. Waiting
// on a node stops nothing else, joins exactly its subtree and pumps only the subtree's
// queues; the root's shutdown_and_wait stops and joins everything. Handles can be copied and
// dropped freely: a parent keeps its children until its own wait joins them. A child_scope
// is a node without a task, for an owner that spawns tasks over its lifetime.

class root_scope;
class child_scope;
class task_handle;
task_handle spawn(MainActorTask<void> task, const root_scope& parent);
task_handle spawn(MainActorTask<void> task, const child_scope& parent);
task_handle spawn(MainActorTask<void> task, const task_handle& parent);

using ErrorHandler = std::function<void(std::exception_ptr)>;

namespace detail {

class task_node {
    using ScopeAssociation =
        decltype(std::declval<stdexec::counting_scope&>().get_token().try_associate());

    // Holds a task in its node's scope and every ancestor's, released from the innermost
    // out, so no ancestor's join can complete before a descendant's.
    class Association {
    public:
        Association() = default;
        Association(Association&&) noexcept = default;
        Association& operator=(Association&&) noexcept = default;
        ~Association() {
            for (auto& scope : scopes)
                scope = {};
        }

        explicit operator bool() const noexcept { return !scopes.empty(); }

        Association try_associate() const noexcept {
            Association copy;
            for (auto& scope : scopes) {
                auto again = scope.try_associate();
                if (!again)
                    return {};
                copy.scopes.push_back(std::move(again));
            }
            return copy;
        }

    private:
        friend class task_node;
        std::vector<ScopeAssociation> scopes;
    };

    // Tasks see the node's plain inplace_stop_token. Nesting stop-forwarding scopes instead
    // fuses the tokens; stdexec::task then keeps a stop source in its own frame, and a stop
    // that completes the task inline on main frees that source during its request_stop.
    struct Token {
        task_node* node;

        Association try_associate() const noexcept {
            Association association;
            auto link = node->scope.get_token().try_associate();
            if (!link)
                return {};
            association.scopes.push_back(std::move(link));
            for (auto ancestor = node->parent.lock(); ancestor; ancestor = ancestor->parent.lock()) {
                link = ancestor->scope.get_token().try_associate();
                if (!link)
                    return {};
                association.scopes.push_back(std::move(link));
            }
            return association;
        }

        template<class Sender>
        auto wrap(Sender&& sender) const {
            return stdexec::write_env(std::forward<Sender>(sender),
                                      stdexec::prop{stdexec::get_stop_token, node->stop.get_token()});
        }
    };

    struct StopRequest {
        stdexec::inplace_stop_source* source;
        void operator()() const noexcept { source->request_stop(); }
    };

    // Owned by the spawned operation, so it ends with it whether the task ran or was dropped
    // unstarted because the tree was already joined.
    struct Lifetime {
        task_node* node;
        ~Lifetime() { node->_taskDone.store(true, std::memory_order_release); }
    };

public:
    // A root: owns the main loop and has no task of its own.
    task_node(Wakeup wakeup, ErrorHandler onError)
    : _loop(std::make_unique<MainThreadLoop>(std::move(wakeup))),
      _onError(std::make_shared<ErrorHandler>(std::move(onError))) {
        _taskDone.store(true, std::memory_order_relaxed);
    }

    explicit task_node(const std::shared_ptr<task_node>& parent)
    : _loop(parent->_loop->make_domain()),
      _onError(parent->_onError),
      parent(parent) {
        _parentLink.emplace(parent->stop.get_token(), StopRequest{&stop});
    }

    task_node(const task_node&) = delete;
    task_node& operator=(const task_node&) = delete;

    ~task_node() {
        // Children spawned after this node was joined were dropped unstarted but still
        // link to its stop source.
        for (auto& child : _children)
            child->_parentLink.reset();
        // Normally joined by an ancestor's wait already; this completes inline then.
        if (!_joined)
            stdexec::sync_wait(scope.join());
    }

    static std::shared_ptr<task_node> spawn(MainActorTask<void> task, const std::shared_ptr<task_node>& parent) {
        auto node = adopt(parent);
        node->launch(std::move(task));
        return node;
    }

    // A node without a task: its subtree is what is spawned under it.
    static std::shared_ptr<task_node> open(const std::shared_ptr<task_node>& parent) {
        auto node = adopt(parent);
        node->_taskDone.store(true, std::memory_order_relaxed);
        return node;
    }

    void close() noexcept { stop.request_stop(); }

    // Joins this subtree, draining only its queues. Serial caller ownership is required
    // per node; close and completed may run concurrently. A subtree an ancestor has already
    // joined returns at once.
    void wait(bool allowNonOwnerDrain) {
        if (_joined)
            return;
        _loop->sync_wait(scope.join(), allowNonOwnerDrain);
        retire();
    }

    [[nodiscard]] bool completed() const noexcept {
        if (!_taskDone.load(std::memory_order_acquire))
            return false;
        std::lock_guard lock(_mutex);
        for (auto& child : _children)
            if (!child->completed())
                return false;
        return true;
    }

    [[nodiscard]] MainThreadLoop::Scheduler scheduler() const noexcept { return _loop->get_scheduler(); }

    void finish() noexcept { _loop->finish(); }

private:
    static std::shared_ptr<task_node> adopt(const std::shared_ptr<task_node>& parent) {
        auto node = std::make_shared<task_node>(parent);
        std::lock_guard lock(parent->_mutex);
        parent->_children.push_back(node);
        return node;
    }

    void launch(MainActorTask<void> task) {
        _taskDone.store(false, std::memory_order_relaxed);
        auto lifetime = std::make_unique<Lifetime>(this);
        auto operation = _loop->bind(std::move(task))
                       | stdexec::upon_error([this](std::exception_ptr error) noexcept { report(std::move(error)); })
                       | stdexec::upon_stopped([]() noexcept {})
                       | stdexec::then([lifetime = std::move(lifetime)]() noexcept {});
        stdexec::spawn(std::move(operation), Token{this});
    }

    // After a join every task in the subtree has finished: mark the scopes joined, unlink
    // the children's stop callbacks and let go of them.
    void retire() noexcept {
        _joined = true;
        std::vector<std::shared_ptr<task_node>> children;
        {
            std::lock_guard lock(_mutex);
            children.swap(_children);
        }
        for (auto& child : children) {
            stdexec::sync_wait(child->scope.join());
            child->retire();
            child->_parentLink.reset();
        }
    }

    void report(std::exception_ptr error) noexcept {
        try {
            (*_onError)(std::move(error));
        } catch (...) {
        }
    }

    std::unique_ptr<MainThreadLoop> _loop;
    std::shared_ptr<ErrorHandler> _onError;
    stdexec::counting_scope scope;
    stdexec::inplace_stop_source stop;
    std::weak_ptr<task_node> parent;
    std::optional<stdexec::inplace_stop_callback<StopRequest>> _parentLink;
    mutable std::mutex _mutex;
    std::vector<std::shared_ptr<task_node>> _children;
    std::atomic<bool> _taskDone{false};
    bool _joined = false;
};

} // namespace detail

// One spawned task and the tasks spawned under it. Copies share the task; dropping all of
// them neither stops nor waits for it.
class task_handle {
public:
    task_handle() = default;

    // Requests stop for this task and its children; never waits.
    void close() const noexcept {
        if (_node)
            _node->close();
    }

    // Joins this task and its children, pumping only their queues. allowNonOwnerDrain lets
    // another thread take them over while the owner is blocked.
    void wait(bool allowNonOwnerDrain = false) const {
        if (_node)
            _node->wait(allowNonOwnerDrain);
    }

    void shutdown_and_wait(bool allowNonOwnerDrain = false) const {
        close();
        wait(allowNonOwnerDrain);
    }

    [[nodiscard]] bool completed() const noexcept {
        return !_node || _node->completed();
    }

private:
    friend class child_scope;
    friend task_handle spawn(MainActorTask<void> task, const root_scope& parent);
    friend task_handle spawn(MainActorTask<void> task, const child_scope& parent);
    friend task_handle spawn(MainActorTask<void> task, const task_handle& parent);

    explicit task_handle(std::shared_ptr<detail::task_node> node) : _node(std::move(node)) {}

    std::shared_ptr<detail::task_node> _node;
};

// Owns the main loop and every task spawned into it. Construct, shut down and destroy it on
// the owner (main) thread; spawn may be called from any thread.
class root_scope {
public:
    explicit root_scope(Wakeup wakeup, ErrorHandler onError = logError)
    : _node(std::make_shared<detail::task_node>(std::move(wakeup), std::move(onError))) {}

    root_scope(const root_scope&) = delete;
    root_scope& operator=(const root_scope&) = delete;

    ~root_scope() { shutdown_and_wait(); }

    // Requests stop for every task, then joins them all, draining every queue on this thread.
    void shutdown_and_wait() {
        if (std::exchange(_joined, true))
            return;
        _node->close();
        _node->wait(false);
        _node->finish();
    }

    [[nodiscard]] bool completed() const noexcept { return _node->completed(); }

    [[nodiscard]] MainThreadLoop::Scheduler scheduler() const noexcept { return _node->scheduler(); }

private:
    friend class child_scope;
    friend task_handle spawn(MainActorTask<void> task, const root_scope& parent);

    static void logError(std::exception_ptr error) {
        try {
            std::rethrow_exception(error);
        } catch (const std::exception& exception) {
            std::cerr << "MainActorTask failed: " << exception.what() << '\n';
        } catch (...) {
            std::cerr << "MainActorTask failed with an unknown exception\n";
        }
    }

    std::shared_ptr<detail::task_node> _node;
    bool _joined = false;
};

// A node without a task of its own, for an owner that spawns tasks over its lifetime (e.g. a
// document that loads in the background): its tasks are stopped and joined with it, without
// touching the rest of the tree. Destroying it shuts it down and waits. Construct, shut down
// and destroy it on the owner (main) thread; spawn may be called from any thread.
class child_scope {
public:
    explicit child_scope(const root_scope& parent) : _node(detail::task_node::open(parent._node)) {}

    explicit child_scope(const task_handle& parent) {
        if (!parent._node)
            throw std::invalid_argument("child_scope requires a valid parent task_handle");
        _node = detail::task_node::open(parent._node);
    }

    child_scope(const child_scope&) = delete;
    child_scope& operator=(const child_scope&) = delete;

    ~child_scope() { shutdown_and_wait(); }

    // Requests stop for every task in this scope; never waits.
    void close() const noexcept { _node->close(); }

    // Joins this scope's tasks, pumping only their queues. After this, tasks spawned into
    // the scope are dropped unstarted.
    void wait(bool allowNonOwnerDrain = false) const { _node->wait(allowNonOwnerDrain); }

    void shutdown_and_wait(bool allowNonOwnerDrain = false) const {
        close();
        wait(allowNonOwnerDrain);
    }

    [[nodiscard]] bool completed() const noexcept { return _node->completed(); }

    [[nodiscard]] MainThreadLoop::Scheduler scheduler() const noexcept { return _node->scheduler(); }

private:
    friend task_handle spawn(MainActorTask<void> task, const child_scope& parent);

    std::shared_ptr<detail::task_node> _node;
};

// Starts the task on its own queue under the parent. An exception escaping it goes to the
// root's error handler. Once the parent has been joined the task is dropped unstarted and
// its handle is completed.
inline task_handle spawn(MainActorTask<void> task, const root_scope& parent) {
    return task_handle{detail::task_node::spawn(std::move(task), parent._node)};
}

inline task_handle spawn(MainActorTask<void> task, const child_scope& parent) {
    return task_handle{detail::task_node::spawn(std::move(task), parent._node)};
}

inline task_handle spawn(MainActorTask<void> task, const task_handle& parent) {
    if (!parent._node)
        throw std::invalid_argument("spawn requires a valid parent task_handle");
    return task_handle{detail::task_node::spawn(std::move(task), parent._node)};
}

} // namespace affine
