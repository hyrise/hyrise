#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {
enum PrintFlags { PrintIgnoreEmptyChunks = 1 << 0, PrintMvcc = 1 << 1 };

/**
 * operator to print the table with its data
 *
 * Note: Print does not support null values at the moment
 */
class Print : public AbstractReadOnlyOperator {
 public:
  explicit Print(const std::shared_ptr<const AbstractOperator> in, std::ostream& out = std::cout, uint32_t flags = 0);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

  static void print(std::shared_ptr<const Table> table, uint32_t flags = 0, std::ostream& out = std::cout);

 protected:
  std::vector<uint16_t> column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<const Table> t) const;
  std::shared_ptr<const Table> _on_execute() override;

  // stream to print the result
  std::ostream& _out;
  uint32_t _flags;
};
}  // namespace opossum
