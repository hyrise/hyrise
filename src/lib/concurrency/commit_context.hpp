#pragma once

#include <atomic>
#include <memory>

#include "types.hpp"

namespace opossum {

// thread-safe
class CommitContext {
 public:
  // creates initial last commit context with cid 0
  CommitContext();
  explicit CommitContext(const CommitID commit_id);

  CommitContext(const CommitContext& rhs) = delete;
  CommitContext& operator=(const CommitContext& rhs) = delete;

  ~CommitContext();

  CommitID commit_id() const;

  bool is_pending() const;
  void make_pending();

  bool has_next() const;

  std::shared_ptr<CommitContext> next();

  // constructs the next context with cid + 1
  std::shared_ptr<CommitContext> get_or_create_next();

 private:
  const CommitID _commit_id;
  std::atomic<bool> _pending;  // true if context is waiting to be committed
  std::shared_ptr<CommitContext> _next;
};
}  // namespace opossum
