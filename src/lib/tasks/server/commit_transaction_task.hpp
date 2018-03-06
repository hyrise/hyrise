#pragma once

#include "abstract_server_task.hpp"

namespace opossum {

class TransactionContext;

class CommitTransactionTask : public AbstractServerTask<void> {
 public:
  explicit CommitTransactionTask(std::shared_ptr<TransactionContext> transaction)
      : _transaction(std::move(transaction)) {}

 protected:
  void _on_execute() override;

  std::shared_ptr<TransactionContext> _transaction;
};

}  // namespace opossum
