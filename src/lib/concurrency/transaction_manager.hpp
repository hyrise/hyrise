#pragma once

#include <atomic>
#include <memory>

#include "commit_context.hpp"
#include "transaction_context.hpp"

#include "types.hpp"

namespace opossum {

// thread-safe
class TransactionManager {
 public:
  static TransactionManager &get();
  static void reset();

  TransactionID next_transaction_id() const;
  CommitID last_commit_id() const;

  std::unique_ptr<TransactionContext> new_transaction_context();

  void abort(TransactionContext &context);
  void prepare_commit(TransactionContext &context);
  void commit(TransactionContext &context);

 private:
  TransactionManager();

  TransactionManager(TransactionManager const &) = delete;
  TransactionManager(TransactionManager &&) = delete;
  TransactionManager &operator=(const TransactionManager &) = delete;
  TransactionManager &operator=(TransactionManager &&) = delete;

  std::shared_ptr<CommitContext> _new_commit_context();
  void _commit(std::shared_ptr<CommitContext> context);

 private:
  std::atomic<TransactionID> _next_transaction_id;
  std::atomic<CommitID> _last_commit_id;

  std::shared_ptr<CommitContext> _last_commit_context;
};
}  // namespace opossum
