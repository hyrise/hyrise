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

  std::unique_ptr<TransactionContext> new_transaction();
  std::shared_ptr<CommitContext> new_commit_context();

  void commit(std::shared_ptr<CommitContext> context);

 private:
  TransactionManager();

  TransactionManager(TransactionManager const &) = delete;
  TransactionManager(TransactionManager &&) = delete;
  TransactionManager &operator=(const TransactionManager &) = delete;
  TransactionManager &operator=(TransactionManager &&) = delete;

 private:
  std::atomic<std::uint32_t> _ntid;
  std::atomic<std::uint32_t> _lcid;

  std::shared_ptr<CommitContext> _lcc;
};
}  // namespace opossum
