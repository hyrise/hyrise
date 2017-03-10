#include "rollback_records.hpp"

#include <memory>
#include <string>

namespace opossum {

const std::string RollbackRecords::name() const { return "RollbackRecords"; }

uint8_t RollbackRecords::num_in_tables() const { return 0; }

uint8_t RollbackRecords::num_out_tables() const { return 0; }

void RollbackRecords::commit_records(const CommitID /*cid*/) {
  throw std::logic_error("RollbackRecords cannot be committed");
}

void RollbackRecords::rollback_records() { /* RollbackRecords calls this method on itself, so we do nothing instead of
                                              throwing exceptions */
}

std::shared_ptr<const Table> RollbackRecords::on_execute(TransactionContext* context) {
  for (const auto op : context->get_rw_operators()) {
    op->rollback_records();
  }
  return nullptr;
}
}  // namespace opossum
