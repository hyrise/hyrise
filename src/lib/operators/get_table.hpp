#pragma once

#include <memory>
#include <string>

#include "abstract_operator.hpp"

namespace opossum {

class get_table : public abstract_operator {
 public:
  explicit get_table(const std::string &name);
  virtual void execute();
  virtual std::shared_ptr<table> get_output() const;

 protected:
  virtual const std::string get_name() const;
  virtual uint8_t get_num_in_tables() const;
  virtual uint8_t get_num_out_tables() const;

  const std::string _name;
};
}  // namespace opossum
