#pragma once

#include <memory>
#include <string>
#include <vector>

#include "storage/table.hpp"

namespace opossum {

/*
 * Class to print a table using pagination in style of the UNIX 'less' command.
 */
class TablePrinter {
 public:
  explicit TablePrinter(std::shared_ptr<const Table> table);

  /*
   * Opens a ncurses environment in which the table is printed.
   * User can navigate through the table with the keyboard (ARROW KEYS, PAGE UP/DOWN, etc.),
   * and quit by pressing 'q'.
   */
  void paginate();

 protected:
  /*
   * Prints out the table header.
   */
  void _print_header();

  /*
   * Prints the chunk header of the given ChunkID
   */
  void _print_chunk_header(const ChunkID chunk_id);

  /*
   * Prints a number of rows (including column/chunk headers if needed) to fill the current terminal screen.
   *
   * @param start_row_id The RowID which should be started from, i.e. which will be the first row on the screen.
   * @returns The RowID of the following row, after the last row printed on the screen.
   *          Returns NULL_ROW_ID if the end of the table is reached.
   */
  RowID _print_screen(const RowID& start_row_id);

  /*
   * Prints the help screen, which shows all available commands.
   * The help screen is displayed on a seperate ncurses window. When the help screen is closed (by pressing 'q'),
   * the seperate window gets destroyed, and the previous contend is restored, showing the table as before.
   */
  void _print_help_screen();

  /*
   * Prints the row of the given RowID.
   */
  void _print_row(const RowID& row_id);

  /*
   * @returns The RowID of the next row from the given RowID.
   *          Returns NULL_ROW_ID if the end of the table is reached.
   */
  RowID _next_row(const RowID& row_id);

  /*
   * @returns The RowID of the previous row from the given RowID.
   *          If called on the first row of the table, it returns NULL_ROW_ID instead.
   */
  RowID _previous_row(const RowID& row_id);

  /*
   * @returns The RowID of the row X rows before the given RowID, where X is the screen size.
   */
  RowID _previous_page(const RowID& row_id);

  /*
   * @returns The RowID of the row X rows before the last possible RowID, where X is the screen size.
   */
  RowID _last_page_start_row();

  /*
   * This prints "\n", and also increases the counter of rows printed by one.
   * This is necessary to keep track of how many rows can be printed to fit on the screen dynamically,
   * i.e. taking column and chunk headers into consideration.
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
  bool _has_mvcc;
  bool _print_column_header;
  size_t _rows_printed;
  size_t _size_x;
  size_t _size_y;
};

}  // namespace opossum
