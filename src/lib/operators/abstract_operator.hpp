#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../common.hpp"
#include "../storage/table.hpp"

namespace opossum {

class AbstractOperator {
 public:
  AbstractOperator(const std::shared_ptr<AbstractOperator> left = nullptr,
                   const std::shared_ptr<AbstractOperator> right = nullptr);
  AbstractOperator(AbstractOperator const&) = delete;
  AbstractOperator(AbstractOperator&&) = default;

  virtual void execute() = 0;
  virtual std::shared_ptr<Table> get_output() const = 0;

 protected:
  virtual const std::string get_name() const = 0;
  virtual uint8_t get_num_in_tables() const = 0;
  virtual uint8_t get_num_out_tables() const = 0;

  const std::shared_ptr<Table> _input_left, _input_right;
};

class AbstractOperatorImpl {
 public:
  virtual void execute() = 0;
  virtual std::shared_ptr<Table> get_output() const = 0;
};
}  // namespace opossum
