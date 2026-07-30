#include "cio/context.hpp"

namespace cio {
namespace {

// Keeps whatever a derived scope needs alive for as long as its state exists:
// the timer that will fire, and the hook registered on the parent.
struct ScopeKeepalive {
    Timer timer;
    std::shared_ptr<detail::CancelHook> parent_hook;
    CancelToken parent;

    ~ScopeKeepalive() {
        // Unregister from the parent, or a long-lived parent would accumulate a
        // hook per finished child.
        if (parent_hook != nullptr) {
            if (const auto& state = detail::CancelAccess::state(parent);
                state != nullptr) {
                state->remove_hook(parent_hook);
            }
        }
    }
};

// Cancels a child when the parent fires.
class ParentLink final : public detail::CancelHook {
public:
    explicit ParentLink(std::weak_ptr<detail::CancelState> child) noexcept
        : child_(std::move(child)) {}

    void on_cancel() noexcept override {
        // Weak: the child may already be gone, and a parent firing must not
        // resurrect it.
        if (auto state = child_.lock()) state->cancel();
    }

private:
    std::weak_ptr<detail::CancelState> child_;
};

void link_to_parent(const CancelSource& child, CancelToken parent,
                    ScopeKeepalive& keepalive) {
    const auto& parent_state = detail::CancelAccess::state(parent);
    if (parent_state == nullptr) return;

    auto hook = std::make_shared<ParentLink>(child.state());
    if (!parent_state->add_hook(hook)) {
        // The parent had already fired, so no callback is coming.
        child.cancel();
        return;
    }
    keepalive.parent_hook = std::move(hook);
    keepalive.parent = std::move(parent);
}

CancelSource make_scope(CancelToken parent, std::int64_t deadline_ns) {
    CancelSource child;
    auto keepalive = std::make_shared<ScopeKeepalive>();

    // Clamp to the parent's deadline: a child must not be able to grant itself
    // more time than the scope it sits inside.
    if (const auto parent_deadline = parent.deadline()) {
        const std::int64_t at =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                parent_deadline->time_since_epoch())
                .count();
        if (deadline_ns == 0 || at < deadline_ns) deadline_ns = at;
    }

    if (deadline_ns != 0) {
        child.state()->deadline_ns.store(deadline_ns,
                                        std::memory_order_release);
        // after_func cancels the scope when the deadline arrives, and the timer
        // lives in the keepalive so it is stopped when the scope goes away.
        auto weak = std::weak_ptr<detail::CancelState>(child.state());
        keepalive->timer = after_func(
            TimePoint{std::chrono::nanoseconds{deadline_ns}} - Clock::now(),
            [weak] {
                if (auto state = weak.lock()) state->cancel();
            });
    }

    link_to_parent(child, std::move(parent), *keepalive);
    child.state()->keepalive = std::move(keepalive);
    return child;
}

}  // namespace

CancelSource with_cancel(CancelToken parent) {
    return make_scope(std::move(parent), 0);
}

CancelSource with_deadline(CancelToken parent, TimePoint deadline) {
    return make_scope(std::move(parent),
                      std::chrono::duration_cast<std::chrono::nanoseconds>(
                          deadline.time_since_epoch())
                          .count());
}

CancelSource with_timeout(CancelToken parent, Duration timeout) {
    return with_deadline(std::move(parent), Clock::now() + timeout);
}

CancelSource with_deadline(TimePoint deadline) {
    return make_scope(CancelToken{},
                      std::chrono::duration_cast<std::chrono::nanoseconds>(
                          deadline.time_since_epoch())
                          .count());
}

CancelSource with_timeout(Duration timeout) {
    return with_deadline(Clock::now() + timeout);
}

}  // namespace cio
