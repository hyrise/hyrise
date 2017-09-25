#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {

/**
 * operator to print the table with its data
 *
 * Note: Print does not support null values at the moment
 */
class Print : public AbstractReadOnlyOperator {
 public:
  explicit Print(const std::shared_ptr<const AbstractOperator> in, std::ostream& out = std::cout,
                 bool ignore_empty_chunks = false);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

  static void print(std::shared_ptr<const Table> table, bool ignore_empty_chunks = false,
                    std::ostream& out = std::cout);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  // stream to print the result
  std::ostream& _out;
  bool _ignore_empty_chunks;
};
}  // namespace opossum
