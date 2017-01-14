#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_non_modifying_operator.hpp"

namespace opossum {
// operator to print the table with its data
class Print : public AbstractNonModifyingOperator {
 public:
  explicit Print(const std::shared_ptr<const AbstractOperator> in, std::ostream& out = std::cout);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::vector<uint16_t> column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<const Table> t) const;
  std::shared_ptr<const Table> on_execute() override;

  // stream to print the result
  std::ostream& _out;
};
}  // namespace opossum
