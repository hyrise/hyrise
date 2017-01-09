#pragma once

#include <atomic>
#include <memory>

namespace opossum {

// thread-safe
class CommitContext {
 public:
  CommitContext();
  CommitContext(const uint32_t cid);
  ~CommitContext();

  uint32_t cid() const;

  bool is_pending() const;
  void make_pending();

  bool has_next() const;
  std::shared_ptr<CommitContext> get_or_create_next();

 private:
  const uint32_t _cid;
  std::atomic<bool> _pending;
  std::shared_ptr<CommitContext> _next;
};
}  // namespace opossum
