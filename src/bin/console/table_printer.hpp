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
   * Prints out specified number of rows starting from a specified RowID.
   * Default parameters make it print out the whole table.
   *
   * @param row_id The RowID to begin from.
   * @param rows   The number of rows to print. If set to maximum of size_t, print the whole table.
   *
   * @returns The RowID which follows the last printed row. Returns NULL_ROW_ID if the last row was printed.
   */
  RowID print(const RowID& row_id, const size_t rows);

  /*
   * Prints out the table header.
   */
  void print_header();

  /*
   * Prints the closing previously specified by set_closing, the default is an empty string.
   */
  void print_closing();
  void set_closing(const std::string& closing);

 protected:
  /*
   * Prints the chunk header of the given ChunkID, and "Empty chunk" if chunk is empty.
   * Does not print anything if chunk is empty and _ignore_empty_chunks==true;
   */
  void _print_chunk_header(const ChunkID chunk_id);

  /*
   *
   */
  void _print_screen(const RowID& start_row_id);

  /*
   * Prints the row of the given RowID.
   */
  void _print_row(const RowID& row_id);

  RowID _next_row(const RowID& row_id);

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
