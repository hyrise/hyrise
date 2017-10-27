#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class UnionAll : public AbstractReadOnlyOperator {
 public:
  UnionAll(const std::shared_ptr<const AbstractOperator> left_in,
           const std::shared_ptr<const AbstractOperator> right_in);
  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<Table> _output;
};
}  // namespace opossum
