#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

// maintenance operator to print a list of table names stored by the StorageManager
class ShowTables : public AbstractReadOnlyOperator {
 public:
  explicit ShowTables(std::ostream& out = std::cout);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

 private:
  std::ostream& _out;
};
}  // namespace opossum
