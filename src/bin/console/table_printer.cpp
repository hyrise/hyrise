#include "table_printer.hpp"

#include <ncurses.h>
#include <inttypes.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "utils/performance_warning.hpp"

namespace opossum {

TablePrinter::TablePrinter(std::shared_ptr<const Table> table, bool ignore_empty_chunks)
    : _table(table),
      _rows_printed(0),
      _closing(""),
      _ignore_empty_chunks(ignore_empty_chunks),
      _print_column_header(true),
      _has_mvcc(false) {
  _widths = _column_string_widths(8, 20);

  for (ChunkID chunk_id{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
    if (_table->get_chunk(chunk_id).has_mvcc_columns()) {
      _has_mvcc = true;
      break;
    }
  }
}

void TablePrinter::paginate() {

  // Init curses
  initscr();
  clear();
  // cbreak();
  keypad(stdscr, TRUE);
  curs_set(0);

  getmaxyx(stdscr, _size_y, _size_x);
  // Last line on the screen should show instructions
  --_size_y;

  // int rowcount = _table->row_count();;

  _print_column_header = true;
  auto start_row = RowID{};
  auto next_page_row = _print_screen(start_row);

  int ch;
  while ((ch = getch()) != 'q') {
    switch (ch) {
      case 'g':
      case '<': {
        _print_column_header = true;
        start_row = RowID{};

        next_page_row = _print_screen(start_row);
        break;
      }

      case 'G':
      case '>': {
        start_row = _last_page_start_row();

        if (start_row == NULL_ROW_ID){
          start_row = RowID{};
          _print_column_header = true;
        } else {
          _print_column_header = false;
        }

        next_page_row = _print_screen(start_row);
        break;
      }

      case KEY_DOWN: {
        if (next_page_row == NULL_ROW_ID) {
          break;
        }

        if (!_print_column_header){
          start_row = _next_row(start_row);
        }
        _print_column_header = false;

        next_page_row = _print_screen(start_row);
        break;
      }

      case KEY_UP: {
        start_row = _previous_row(start_row);

        if (start_row == NULL_ROW_ID){
          start_row = RowID{};
          _print_column_header = true;
        }

        next_page_row = _print_screen(start_row);
        break;
      }

      case ' ':
      case KEY_NPAGE: {
        if (next_page_row == NULL_ROW_ID) {
          break;
        }

        _print_column_header = false;
        start_row = next_page_row;

        next_page_row = _print_screen(start_row);
        break;
      }

      case 'b':
      case KEY_PPAGE: {
        start_row = _previous_page(start_row);

        if (start_row == NULL_ROW_ID)
        {
          start_row = RowID{};
          _print_column_header = true;
        }

        next_page_row = _print_screen(start_row);
        break;
      }
    }
  }

  endwin();
}

RowID TablePrinter::_next_row(const RowID& row_id) {
  ChunkID new_chunk_id = row_id.chunk_id;
  ChunkOffset new_chunk_offset = ChunkOffset{row_id.chunk_offset + 1};

  if (new_chunk_offset >= _table->get_chunk(new_chunk_id).size()) {
    new_chunk_id = ChunkID{new_chunk_id + 1};
    new_chunk_offset = ChunkOffset{0u};
    if (new_chunk_id >= _table->chunk_count()) {
      return NULL_ROW_ID;
    }
  }

  return RowID{new_chunk_id, new_chunk_offset};
}

RowID TablePrinter::_previous_row(const RowID& row_id) {
  if (row_id == NULL_ROW_ID) {
    return NULL_ROW_ID;
  }
  if (row_id.chunk_offset == 0u) {
    if (row_id.chunk_id == 0u) {
      return NULL_ROW_ID;
    }
    return RowID{ChunkID{row_id.chunk_id - 1}, ChunkOffset{row_id.chunk_offset}};
  }
  return RowID{ChunkID{row_id.chunk_id}, ChunkOffset{row_id.chunk_offset - 1}};
}

RowID TablePrinter::_previous_page(const RowID& row_id) {
  RowID start_row = row_id;
  for (size_t i = 0; i < _size_y; ++i) {
    start_row = _previous_row(start_row);
  }
  return start_row;
}

RowID TablePrinter::_last_page_start_row() {
  ChunkID chunk_id = ChunkID{_table->chunk_count() - 1};
  ChunkOffset chunk_offset = ChunkOffset{_table->get_chunk(chunk_id).size()};

  return _previous_page(RowID{chunk_id, chunk_offset});
}

void TablePrinter::_print_header() {
  // std::cout << "=== Columns" << std::endl;
  printw("=== Columns");
  _end_line();
  for (ColumnID col{0}; col < _table->col_count(); ++col) {
    // std::cout << "|" << std::setw(_widths[col]) << _table->column_name(col) << std::setw(0);
    printw("|%-*s", _widths[col], _table->column_name(col).c_str());
  }
  if (_has_mvcc) {
    // std::cout << "||        MVCC        ";
    printw("||        MVCC        ");
  }
  // std::cout << "|" << std::endl;
  printw("|");
  _end_line();
  for (ColumnID col{0}; col < _table->col_count(); ++col) {
    // std::cout << "|" << std::setw(_widths[col]) << _table->column_type(col) << std::setw(0);
    printw("|%-*s", _widths[col], _table->column_type(col).c_str());
  }
  if (_has_mvcc) {
    // std::cout << "||_BEGIN|_END  |_TID  ";
    printw("||_BEGIN|_END  |_TID  ");
  }
  // std::cout << "|" << std::endl;
  printw("|");
  _end_line();
}

void TablePrinter::print_closing() { std::cout << _closing << std::endl; }

void TablePrinter::set_closing(const std::string& closing) { _closing = closing; }

void TablePrinter::_print_chunk_header(const ChunkID chunk_id) {
  auto& chunk = _table->get_chunk(chunk_id);
  if (chunk.size() == 0 && (_ignore_empty_chunks)) {
    return;
  }

  // std::cout << "=== Chunk " << chunk_id << " === " << std::endl;
  printw("=== Chunk %" PRIu32 " === ", (uint32_t) chunk_id);
  _end_line();

  if (chunk.size() == 0) {
    // std::cout << "Empty chunk." << std::endl;
    printw("Empty chunk.");
    _end_line();
    return;
  }
}

RowID TablePrinter::_print_screen(const RowID& start_row_id) {
  clear();
  _rows_printed = 0;

  if (_print_column_header) {
    _print_header();
  }

  auto row_id = start_row_id;

  while (_rows_printed < _size_y) {
    _print_row(row_id);
    row_id = _next_row(row_id);
    if (row_id == NULL_ROW_ID) {
      break;
    }
  }

  refresh();
  return row_id;
}

void TablePrinter::_print_row(const RowID& row_id) {
  PerformanceWarningDisabler pwd;

  auto& chunk = _table->get_chunk(row_id.chunk_id);
  ChunkOffset row = row_id.chunk_offset;

  if (row == 0) {
    _print_chunk_header(row_id.chunk_id);

    if (chunk.size() == 0) {
      return;
    }
  }

  // std::cout << "|";
  printw("|");
  for (ColumnID col{0}; col < chunk.col_count(); ++col) {
    // well yes, we use BaseColumn::operator[] here, but since print is not an operation that should
    // be part of a regular query plan, let's keep things simple here
    // std::cout << std::setw(_widths[col]) << (*chunk.get_column(col))[row] << "|" << std::setw(0);
    printw("%-*s%s", _widths[col], boost::lexical_cast<std::string>((*chunk.get_column(col))[row]).c_str(), "|");
  }

  if (_has_mvcc) {
    auto mvcc_columns = chunk.mvcc_columns();

    auto begin = mvcc_columns->begin_cids[row];
    auto end = mvcc_columns->end_cids[row];
    auto tid = mvcc_columns->tids[row];

    auto begin_str = begin == Chunk::MAX_COMMIT_ID ? "" : std::to_string(begin);
    auto end_str = end == Chunk::MAX_COMMIT_ID ? "" : std::to_string(end);
    auto tid_str = tid == 0 ? "" : std::to_string(tid);

    // std::cout << "|" << std::setw(6) << begin_str << std::setw(0);
    printw("|%-6s", begin_str.c_str());
    // std::cout << "|" << std::setw(6) << end_str << std::setw(0);
    printw("|%-6s", end_str.c_str());
    // std::cout << "|" << std::setw(6) << tid_str << std::setw(0);
    printw("|%-6s", tid_str.c_str());
    // std::cout << "|";
    printw("|");
  }
  // std::cout << std::endl;
  _end_line();
}

void TablePrinter::_end_line() {
  printw("\n");
  ++_rows_printed;
}

// In order to print the table as an actual table, with columns being aligned, we need to calculate the
// number of characters in the printed representation of each column
// `min` and `max` can be used to limit the width of the columns - however, every column fits at least the column's name
std::vector<uint16_t> TablePrinter::_column_string_widths(uint16_t min, uint16_t max) const {
  PerformanceWarningDisabler pwd;
  std::vector<uint16_t> widths(_table->col_count());
  // calculate the length of the column name
  for (ColumnID col{0}; col < _table->col_count(); ++col) {
    widths[col] = std::max(min, static_cast<uint16_t>(to_string(_table->column_name(col)).size()));
  }

  // go over all rows and find the maximum length of the printed representation of a value, up to max
  for (ChunkID chunk_id{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
    auto& chunk = _table->get_chunk(chunk_id);

    for (ColumnID col{0}; col < chunk.col_count(); ++col) {
      for (size_t row = 0; row < chunk.size(); ++row) {
        auto cell_length = static_cast<uint16_t>(to_string((*chunk.get_column(col))[row]).size());
        widths[col] = std::max({min, widths[col], std::min(max, cell_length)});
      }
    }
  }
  return widths;
}

}  // namespace opossum
