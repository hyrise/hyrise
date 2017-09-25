#include "table_printer.hpp"

// #include <algorithm>
#include <iomanip>
#include <iostream>
// #include <memory>
#include <string>
// #include <vector>

// #include "storage/base_column.hpp"
// #include "type_cast.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

TablePrinter::TablePrinter(std::shared_ptr<const Table> table, std::ostream& out, bool ignore_empty_chunks)
    : _table(table), _out(out), _rows_printed(0), _closing(""), _ignore_empty_chunks(ignore_empty_chunks), _has_mvcc(false) {
  _widths = _column_string_widths(8, 20, _table);
  
  for (ChunkID chunk_id{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
    if (_table->get_chunk(chunk_id).has_mvcc_columns()) {
      _has_mvcc = true;
    }
  }
}

RowID TablePrinter::print(const RowID & row_id, const size_t rows) {
  RowID row = row_id;

  size_t rows_printed = 0;
  while (rows_printed < rows) {
    if (row.chunk_id >= _table->chunk_count()) {
      return NULL_ROW_ID;
    }

    uint32_t chunk_size = _table->get_chunk(row.chunk_id).size();

    if (row.chunk_offset == 0) {
      _print_chunk_header(row.chunk_id);

      if (chunk_size == 0) {
        row = RowID{ChunkID{row.chunk_id + 1}, ChunkOffset{0}};
        continue;
      }
    }

    _print_row(row);

    row = RowID{ChunkID{row.chunk_id}, ChunkOffset{row.chunk_offset + 1}};
    if (row.chunk_offset >= chunk_size) {
      row = RowID{ChunkID{row.chunk_id + 1}, ChunkOffset{0}};
    }

    ++rows_printed;
  }

  return row;
}

void TablePrinter::print_header() {
  _out << "=== Columns" << std::endl;
  for (ColumnID col{0}; col < _table->col_count(); ++col) {
    _out << "|" << std::setw(_widths[col]) << _table->column_name(col) << std::setw(0);
  }
  if (_has_mvcc) {
    _out << "||        MVCC        ";
  }
  _out << "|" << std::endl;
  for (ColumnID col{0}; col < _table->col_count(); ++col) {
    _out << "|" << std::setw(_widths[col]) << _table->column_type(col) << std::setw(0);
  }
  if (_has_mvcc) {
    _out << "||_BEGIN|_END  |_TID  ";
  }
  _out << "|" << std::endl;
}

void TablePrinter::print_closing() {
  _out << _closing << std::endl;
}

void TablePrinter::set_closing(const std::string & closing) {
  _closing = closing;
}

void TablePrinter::_print_chunk_header(const ChunkID chunk_id) {
  auto& chunk = _table->get_chunk(chunk_id);
  if (chunk.size() == 0 && (_ignore_empty_chunks)) {
    return;
  }

  _out << "=== Chunk " << chunk_id << " === " << std::endl;

  if (chunk.size() == 0) {
    _out << "Empty chunk." << std::endl;
    return;
  }
}

void TablePrinter::_print_row(const RowID & row_id) {
  PerformanceWarningDisabler pwd;

  auto& chunk = _table->get_chunk(row_id.chunk_id);
  ChunkOffset row = row_id.chunk_offset;

  _out << "|";
  for (ColumnID col{0}; col < chunk.col_count(); ++col) {
    // well yes, we use BaseColumn::operator[] here, but since print is not an operation that should
    // be part of a regular query plan, let's keep things simple here
    _out << std::setw(_widths[col]) << (*chunk.get_column(col))[row] << "|" << std::setw(0);
  }

  if (_has_mvcc) {
    auto mvcc_columns = chunk.mvcc_columns();

    auto begin = mvcc_columns->begin_cids[row];
    auto end = mvcc_columns->end_cids[row];
    auto tid = mvcc_columns->tids[row];

    auto begin_str = begin == Chunk::MAX_COMMIT_ID ? "" : std::to_string(begin);
    auto end_str = end == Chunk::MAX_COMMIT_ID ? "" : std::to_string(end);
    auto tid_str = tid == 0 ? "" : std::to_string(tid);

    _out << "|" << std::setw(6) << begin_str << std::setw(0);
    _out << "|" << std::setw(6) << end_str << std::setw(0);
    _out << "|" << std::setw(6) << tid_str << std::setw(0);
    _out << "|";
  }
  _out << std::endl;
}

// In order to print the table as an actual table, with columns being aligned, we need to calculate the
// number of characters in the printed representation of each column
// `min` and `max` can be used to limit the width of the columns - however, every column fits at least the column's name
std::vector<uint16_t> TablePrinter::_column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<const Table> t) const {
  PerformanceWarningDisabler pwd;
  std::vector<uint16_t> widths(t->col_count());
  // calculate the length of the column name
  for (ColumnID col{0}; col < t->col_count(); ++col) {
    widths[col] = std::max(min, static_cast<uint16_t>(to_string(t->column_name(col)).size()));
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
