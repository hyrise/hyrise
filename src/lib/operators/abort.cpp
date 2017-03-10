#include "abort.hpp"

#include <memory>
#include <string>

namespace opossum {

const std::string Abort::name() const { return "Abort"; }

uint8_t Abort::num_in_tables() const { return 0; }

uint8_t Abort::num_out_tables() const { return 0; }

void Abort::commit(const CommitID /*cid*/) { throw std::logic_error("Abort cannot be committed"); }

void Abort::abort() { /* Abort calls this method on itself, so we do nothing instead of throwing exceptions */
}

std::shared_ptr<const Table> Abort::on_execute(TransactionContext* context) {
  for (const auto op : context->get_rw_operators()) {
    op->abort();
  }
  return nullptr;
}
}  // namespace opossum
