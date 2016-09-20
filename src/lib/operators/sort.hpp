#pragma once

#include <memory>

#include "abstract_operator.hpp"

namespace opossum {
class Sort : public AbstractOperator {
 public:
  Sort(const std::shared_ptr<AbstractOperator> in, const std::string &sort_column_name, const bool ascending = true);
  virtual void execute();
  virtual std::shared_ptr<Table> get_output() const;

 protected:
  virtual const std::string get_name() const;
  virtual uint8_t get_num_in_tables() const;
  virtual uint8_t get_num_out_tables() const;
};
}