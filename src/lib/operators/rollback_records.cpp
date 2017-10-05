#include "rollback_records.hpp"

#include <memory>
#include <string>

#include "utils/assert.hpp"

namespace opossum {

const std::string RollbackRecords::name() const { return "RollbackRecords"; }

uint8_t RollbackRecords::num_in_tables() const { return 0; }

uint8_t RollbackRecords::num_out_tables() const { return 0; }

void RollbackRecords::commit_records(const CommitID /*cid*/) { Fail("RollbackRecords cannot be committed"); }

// RollbackRecords calls this method on itself, so we do nothing instead of throwing exceptions.
void RollbackRecords::rollback_records() {}

std::shared_ptr<const Table> RollbackRecords::_on_execute(std::shared_ptr<TransactionContext> context) {
  for (const auto op : context->get_rw_operators()) {
    op->rollback_records();
  }
  return nullptr;
}
}  // namespace opossum
