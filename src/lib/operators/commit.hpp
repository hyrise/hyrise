#pragma once

#include <memory>
#include <string>

#include "abstract_read_write_operator.hpp"

namespace opossum {

// operator to commit all operators in the current commit context.
class Commit : public AbstractReadWriteOperator {
 public:
  Commit();
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  void commit(const uint32_t cid) override;
  void abort() override;

 protected:
  /**
   * Calls commit on all read-write operators. Needs to have prepare_commit calle first.
   */
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;
};
}  // namespace opossum
