#pragma once

#include <memory>
#include <string>

#include "abstract_read_write_operator.hpp"

namespace opossum {

// operator to abort all operators in the current commit context.
class Abort : public AbstractReadWriteOperator {
 public:
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  void commit(const uint32_t cid) override;
  void abort() override;

 protected:
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;
};
}  // namespace opossum
