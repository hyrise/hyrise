#pragma once

#include <atomic>
#include <memory>

#include "commit_context.hpp"
#include "types.hpp"

namespace opossum {

enum class TransactionPhase { Active, Aborted, Committing, Done };

class Transaction {
 public:
  Transaction(const uint32_t tid, const uint32_t lcid);
  ~Transaction();

  uint32_t tid() const;
  uint32_t lcid() const;

  TransactionPhase phase() const;

  void append_inserted_row(const std::string& table, const RowID row_id);
  void append_deleted_row(const std::string& table, const RowID row_id);

  void abort();
  void prepareCommit();
  void commit();

 private:
  const uint32_t _tid;
  const uint32_t _lcid;

  TransactionPhase _phase;

  std::vector<std::pair<std::string, RowID>> _inserted_rows;
  std::vector<std::pair<std::string, RowID>> _deleted_rows;

  std::shared_ptr<CommitContext> _commit_context;
};
}  // namespace opossum
