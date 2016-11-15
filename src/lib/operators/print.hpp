#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"

namespace opossum {
// operator to print the table with its data
class Print : public AbstractOperator {
 public:
  explicit Print(const std::shared_ptr<const AbstractOperator> in);
  explicit Print(const std::shared_ptr<const AbstractOperator> in, std::ostream& out);
  virtual void execute();
  virtual std::shared_ptr<const Table> get_output() const;

  virtual const std::string name() const;
  virtual uint8_t num_in_tables() const;
  virtual uint8_t num_out_tables() const;

 protected:
  std::vector<uint16_t> column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<const Table> t) const;

  // stream to print the result
  std::ostream& _out = std::cout;
};
}  // namespace opossum
