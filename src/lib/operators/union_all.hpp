#pragma once

#include <memory>
#include <string>

#include "abstract_operator.hpp"
#include "types.hpp"

namespace opossum {

class UnionAll : public AbstractOperator {
 public:
  UnionAll(const std::shared_ptr<const AbstractOperator> left_in,
           const std::shared_ptr<const AbstractOperator> right_in);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  std::shared_ptr<Table> _output;
};
}  // namespace opossum
