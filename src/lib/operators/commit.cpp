#include "commit.hpp"

#include <memory>
#include <string>

namespace opossum {
Commit::Commit() {}
const std::string Commit::name() const { return "Commit"; }

uint8_t Commit::num_in_tables() const { return 0; }

uint8_t Commit::num_out_tables() const { return 0; }

void Commit::commit(const CommitID /*cid*/) {}
void Commit::abort() {}

std::shared_ptr<const Table> Commit::on_execute(TransactionContext* context) {
  for (const auto op : context->get_rw_operators()) {
    op->commit(context->commit_id());
  }
  return nullptr;
}
}  // namespace opossum
