#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "types.hpp"

namespace opossum {

/**
 * Data structure that ensures transaction are committed in an orderly manner.
 * Its main purpose is to manage commit ids.
 * It is effectively part of the TransactionContext
 *
 * Should not be used outside the concurrency module!
 */
class CommitContext : private Noncopyable {
 public:
  explicit CommitContext(const CommitID commit_id);

  CommitID commit_id() const;

  bool is_pending() const;

  /**
   * Marks the commit context as “pending”, i.e. ready to be committed
   * as soon as all previous pending have been committed.
   *
   * @param callback called when transaction is committed
   */
  void make_pending(const TransactionID transaction_id, const std::function<void(TransactionID)>& callback = nullptr);

  /**
   * Calls the callback of make_pending
   */
  void fire_callback();

  bool has_next() const;

  std::shared_ptr<CommitContext> next();
  std::shared_ptr<const CommitContext> next() const;

  /**
   * Tries to set the next context. Returns false if it has already
   * been set. Throws error if its commit id isn't equal to this context's
   * commit id + 1.
   */
  bool try_set_next(const std::shared_ptr<CommitContext>& next);

 private:
  const CommitID _commit_id;
  std::atomic<bool> _pending;  // true if context is waiting to be committed
  std::shared_ptr<CommitContext> _next;
  std::function<void()> _callback;
};
}  // namespace opossum
