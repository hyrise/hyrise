#include "commit_context.hpp"

#include <memory>

namespace opossum {

CommitContext::CommitContext() : _cid{0u}, _pending{false} {}

CommitContext::CommitContext(const uint32_t cid) : _cid{cid}, _pending{false} {}

CommitContext::~CommitContext() = default;

uint32_t CommitContext::cid() const { return _cid; }

bool CommitContext::is_pending() const { return _pending; }

void CommitContext::make_pending() { _pending = true; }

bool CommitContext::has_next() const {
  const auto next_copy = std::atomic_load(&_next);
  return _next != nullptr;
}

std::shared_ptr<CommitContext> CommitContext::next() { return std::atomic_load(&_next); }

std::shared_ptr<CommitContext> CommitContext::get_or_create_next() {
  if (has_next()) return next();

  auto new_next = std::make_shared<CommitContext>(_cid + 1);

  auto context_nullptr = std::shared_ptr<CommitContext>();
  std::atomic_compare_exchange_strong(&_next, &context_nullptr, new_next);

  return std::atomic_load(&_next);
}

}  // namespace opossum
