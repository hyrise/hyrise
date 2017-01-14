#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_non_modifying_operator.hpp"
#include "types.hpp"

namespace opossum {

// operator to select a subset of the set of all columns found in the table
class Projection : public AbstractNonModifyingOperator {
 public:
  Projection(const std::shared_ptr<const AbstractOperator> in, const std::vector<std::string> &columns);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  // list of all column names to select
  const std::vector<std::string> _column_filter;
};
}  // namespace opossum
