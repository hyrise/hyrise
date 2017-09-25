#pragma once

#include <fstream>
#include <memory>
#include <vector>

#include "storage/table.hpp"

namespace opossum {

/*
 * 
 */
class TablePrinter {
 public:
  explicit TablePrinter(std::shared_ptr<const Table> table, std::ostream& out = std::cout, bool ignore_empty_chunks = false);

  RowID print(const RowID & row_id, const size_t rows);
  void print_header();
  void print_closing();
  void set_closing(const std::string & closing);

protected:
  void _print_chunk_header(const ChunkID chunk_id);
  void _print_row(const RowID & row_id);
  std::vector<uint16_t> _column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<const Table> t) const;

  const std::shared_ptr<const Table> _table;
  std::ostream& _out;
  std::vector<uint16_t> _widths;
  size_t _rows_printed;
  std::string _closing;
  bool _ignore_empty_chunks;
  bool _has_mvcc;
};

}  // namespace opossum
