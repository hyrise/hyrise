#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {

/**
 * PrintFlags::Mvcc:                   If set, print begin commit id and end commit id and transaction id for each tuple
 * PrintFlags::IgnoreChunkBoundaries:  If set, print a logical view of the Table, i.e., do not print info about Chunks or
 *                              Segment types.
 */
enum class PrintFlags : uint32_t { None = 0u, Mvcc = 1u << 0u, IgnoreChunkBoundaries = 1u << 1u };

/**
 * operator to print the table with its data
 */
class Print : public AbstractReadOnlyOperator {
 public:
  explicit Print(const std::shared_ptr<const AbstractOperator>& in, const PrintFlags flags = PrintFlags::None,
                 std::ostream& out = std::cout);

  const std::string& name() const override;

  static void print(const std::shared_ptr<const Table>& table, const PrintFlags flags = PrintFlags::None,
                    std::ostream& out = std::cout);
  static void print(const std::shared_ptr<const AbstractOperator>& in, const PrintFlags flags = PrintFlags::None,
                    std::ostream& out = std::cout);

  // Convenience method to print the result of an SQL query
  static void print(const std::string& sql, const PrintFlags flags = PrintFlags::None, std::ostream& out = std::cout);

 protected:
  std::vector<uint16_t> _column_string_widths(uint16_t min, uint16_t max,
                                              const std::shared_ptr<const Table>& table) const;
  static std::string _truncate_cell(const AllTypeVariant& cell, uint16_t max_width);
  static std::string _segment_type(const std::shared_ptr<AbstractSegment>& segment);
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  PrintFlags _flags;
  std::ostream& _out;

  static constexpr uint16_t _min_cell_width = 8;
  static constexpr uint16_t _max_cell_width = 20;
};
}  // namespace opossum
