#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "types.hpp"

namespace opossum {

// operator to select a subset of the set of all columns found in the table
class Projection : public AbstractOperator {
 public:
  Projection(const std::shared_ptr<const AbstractOperator> in, const std::vector<std::string> &columns);
  void execute() override;
  std::shared_ptr<const Table> get_output() const override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  const std::shared_ptr<const Table> _table;

  // list of all column names to select
  const std::vector<std::string> _column_filter;
  std::shared_ptr<Table> _output;
};
}  // namespace opossum
