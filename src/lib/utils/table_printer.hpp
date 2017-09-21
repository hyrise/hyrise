#pragma once

#include <fstream>
// #include <functional>
#include <memory>
// #include <string>
// #include <unordered_map>
#include <vector>

#include "storage/table.hpp"

namespace opossum {

/*
 * 
 */
class TablePrinter {
 public:
  explicit TablePrinter(std::shared_ptr<const Table> table, std::ostream& out = std::cout, bool ignore_empty_chunks = false);

  void print();
  // void print_incremental(size_t row_count = 1);

protected:
  void _print_header();
  void _print_row(const Chunk & chunk, const size_t row);
  std::vector<uint16_t> _column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<const Table> t) const;

  const std::shared_ptr<const Table> _table;
  std::ostream& _out;
  std::vector<uint16_t> _widths;
  size_t rows_printed;
  bool _ignore_empty_chunks;
  bool _has_mvcc;
};

}  // namespace opossum
