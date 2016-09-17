#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"

namespace opossum {
class Print : public AbstractOperator {
 public:
  explicit Print(const std::shared_ptr<AbstractOperator> in);
  explicit Print(const std::shared_ptr<AbstractOperator> in, std::ostream& out);
  virtual void execute();
  virtual std::shared_ptr<Table> get_output() const;

 protected:
  virtual const std::string get_name() const;
  virtual uint8_t get_num_in_tables() const;
  virtual uint8_t get_num_out_tables() const;
  std::vector<uint16_t> column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<Table> t) const;

 private:
  std::ostream& _out = std::cout;
};
}  // namespace opossum
