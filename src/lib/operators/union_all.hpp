#pragma once

#include <memory>
#include <string>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"

namespace opossum {

class UnionAll : public AbstractReadOnlyOperator {
 public:
  UnionAll(const std::shared_ptr<const AbstractOperator> left_in,
           const std::shared_ptr<const AbstractOperator> right_in);
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate() const override {
    throw std::runtime_error("Operator " + this->name() + " does not implement recreation.");
  }

 protected:
  std::shared_ptr<const Table> on_execute() override;

  std::shared_ptr<Table> _output;
};
}  // namespace opossum
