#include "commit_records.hpp"

#include <memory>
#include <string>

#include "utils/assert.hpp"

namespace opossum {
CommitRecords::CommitRecords() {}
const std::string CommitRecords::name() const { return "CommitRecords"; }

uint8_t CommitRecords::num_in_tables() const { return 0; }

uint8_t CommitRecords::num_out_tables() const { return 0; }

void CommitRecords::commit_records(const CommitID /*cid*/) {
  /* CommitRecords calls this method on itself, so we do nothing instead of throwing exceptions */
}

void CommitRecords::rollback_records() { Fail("CommitRecords cannot be rolled back"); }

std::shared_ptr<const Table> CommitRecords::_on_execute(std::shared_ptr<TransactionContext> context) {
  for (const auto op : context->get_rw_operators()) {
    op->commit(context->commit_id());
  }
  return nullptr;
}
}  // namespace opossum
