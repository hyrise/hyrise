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
  explicit Print(const std::shared_ptr<const AbstractOperator>& in, std::ostream& out = std::cout, uint32_t flags = 0);

  const std::string name() const override;

  static void print(const std::shared_ptr<const Table>& table, uint32_t flags = 0, std::ostream& out = std::cout);
  static void print(const std::shared_ptr<const AbstractOperator>& in, uint32_t flags = 0,
                    std::ostream& out = std::cout);

 protected:
  std::vector<uint16_t> _column_string_widths(uint16_t min, uint16_t max,
                                              const std::shared_ptr<const Table>& table) const;
  std::string _truncate_cell(const AllTypeVariant& cell, uint16_t max_width) const;
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // stream to print the result
  std::ostream& _out;
  uint32_t _flags;
};
}  // namespace opossum
