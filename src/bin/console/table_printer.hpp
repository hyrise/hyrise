#pragma once

#include <memory>
#include <string>
#include <vector>

#include "storage/table.hpp"

namespace opossum {

/*
 * Class to print a table as a whole or line by line.
 */
class TablePrinter {
 public:
  explicit TablePrinter(std::shared_ptr<const Table> table, bool ignore_empty_chunks = false);

  void paginate();

  /*
   * Prints the closing previously specified by set_closing, the default is an empty string.
   */
  void print_closing();
  void set_closing(const std::string& closing);

 protected:
  /*
   * Prints out the table header.
   */
  void _print_header();

  /*
   * Prints the chunk header of the given ChunkID, and "Empty chunk" if chunk is empty.
   * Does not print anything if chunk is empty and _ignore_empty_chunks==true;
   */
  void _print_chunk_header(const ChunkID chunk_id);

  /*
   *
   */
  RowID _print_screen(const RowID& start_row_id, const bool print_header = false);

  /*
   * Prints the row of the given RowID.
   */
  void _print_row(const RowID& row_id);

  RowID _next_row(const RowID& row_id);
  RowID _previous_row(const RowID& row_id);
  RowID _previous_page(const RowID& row_id);

  /*
   *
   */
  void _end_line();

  /*
   * Calculates the number of characters in the printed representation of each column.
   *
   * @param min Minimum width of each column.
   * @param max Maximum width of each column (though every column fits at least its name).
   *
   * @returns Vector containing the column widths.
   */
  std::vector<uint16_t> _column_string_widths(uint16_t min, uint16_t max) const;

  const std::shared_ptr<const Table> _table;
  std::vector<uint16_t> _widths;
  size_t _rows_printed;
  std::string _closing;
  bool _ignore_empty_chunks;
  bool _has_mvcc;
  size_t _size_x;
  size_t _size_y;
};

}  // namespace opossum
