#pragma once

#include <atomic>
#include <memory>

#include "commit_context.hpp"
#include "transaction_context.hpp"

namespace opossum {

// thread-safe
class TransactionManager {
 public:
  static TransactionManager &get();
  static void reset();

  uint32_t ntid() const;
  uint32_t lcid() const;

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

  std::shared_ptr<CommitContext> new_commit_context();
  void commit(std::shared_ptr<CommitContext> context);

 private:
  std::atomic<uint32_t> _ntid;
  std::atomic<uint32_t> _lcid;

  std::shared_ptr<CommitContext> _lcc;
};
}  // namespace opossum
