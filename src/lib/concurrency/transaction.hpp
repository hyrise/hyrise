#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <vector>

#include "commit_context.hpp"
#include "operators/abstract_modifying_operator.hpp"

namespace opossum {

enum class TransactionPhase { Active, Aborting, Aborted, Committing, Done };

class Transaction {
 public:
  Transaction(const uint32_t tid, const uint32_t lcid);
  ~Transaction();

  uint32_t tid() const;
  uint32_t lcid() const;

  TransactionPhase phase() const;

  void add_operator(const std::shared_ptr<AbstractModifyingOperator>& op);

  void abort();

  // TODO: rename to prepare_commit ?
  void commit();

 private:
  const uint32_t _tid;
  const uint32_t _lcid;

  TransactionPhase _phase;
  std::vector<std::shared_ptr<AbstractModifyingOperator>> _operators;
  std::shared_ptr<CommitContext> _commit_context;
};
}  // namespace opossum
