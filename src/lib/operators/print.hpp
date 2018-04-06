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
 */
class Print : public AbstractReadOnlyOperator {
 public:
  explicit Print(const AbstractOperatorCSPtr in, std::ostream& out = std::cout, uint32_t flags = 0);

  const std::string name() const override;

  static void print(TableCSPtr table, uint32_t flags = 0, std::ostream& out = std::cout);
  static void print(AbstractOperatorCSPtr in, uint32_t flags = 0, std::ostream& out = std::cout);

 protected:
  std::vector<uint16_t> _column_string_widths(uint16_t min, uint16_t max, TableCSPtr t) const;
  std::string _truncate_cell(const AllTypeVariant& cell, uint16_t max_width) const;
  TableCSPtr _on_execute() override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

  // stream to print the result
  std::ostream& _out;
  uint32_t _flags;
};
}  // namespace opossum
