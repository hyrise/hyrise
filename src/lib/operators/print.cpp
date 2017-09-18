#include "print.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "operators/table_wrapper.hpp"
#include "storage/base_column.hpp"
#include "type_cast.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

Print::Print(const std::shared_ptr<const AbstractOperator> in, std::ostream& out, uint32_t flags)
    : AbstractReadOnlyOperator(in), _out(out), _flags(flags) {}

const std::string Print::name() const { return "Print"; }

uint8_t Print::num_in_tables() const { return 1; }

uint8_t Print::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> Print::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<Print>(_input_left->recreate(args), _out);
}

void Print::print(std::shared_ptr<const Table> table, uint32_t flags, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, out, flags).execute();
}

std::shared_ptr<const Table> Print::_on_execute() {
  PerformanceWarningDisabler pwd;

  auto widths = column_string_widths(8, 20, _input_table_left());

  // print column headers
  _out << "=== Columns" << std::endl;
  for (ColumnID col{0}; col < _input_table_left()->col_count(); ++col) {
    _out << "|" << std::setw(widths[col]) << _input_table_left()->column_name(col) << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||        MVCC        ";
  }
  _out << "|" << std::endl;
  for (ColumnID col{0}; col < _input_table_left()->col_count(); ++col) {
    _out << "|" << std::setw(widths[col]) << _input_table_left()->column_type(col) << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||_BEGIN|_END  |_TID  ";
  }
  _out << "|" << std::endl;

  // print each chunk
  for (ChunkID chunk_id{0}; chunk_id < _input_table_left()->chunk_count(); ++chunk_id) {
    auto& chunk = _input_table_left()->get_chunk(chunk_id);
    if (chunk.size() == 0 && (_flags & PrintIgnoreEmptyChunks)) {
      continue;
    }

    _out << "=== Chunk " << chunk_id << " === " << std::endl;

    if (chunk.size() == 0) {
      _out << "Empty chunk." << std::endl;
      continue;
    }

    // print the rows in the chunk
    for (size_t row = 0; row < chunk.size(); ++row) {
      _out << "|";
      for (ColumnID col{0}; col < chunk.col_count(); ++col) {
        // well yes, we use BaseColumn::operator[] here, but since Print is not an operation that should
        // be part of a regular query plan, let's keep things simple here
        _out << std::setw(widths[col]) << (*chunk.get_column(col))[row] << "|" << std::setw(0);
      }

      if (_flags & PrintMvcc && chunk.has_mvcc_columns()) {
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
  }

  return _input_table_left();
}

// In order to print the table as an actual table, with columns being aligned, we need to calculate the
// number of characters in the printed representation of each column
// `min` and `max` can be used to limit the width of the columns - however, every column fits at least the column's name
std::vector<uint16_t> Print::column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<const Table> t) const {
  std::vector<uint16_t> widths(t->col_count());
  // calculate the length of the column name
  for (ColumnID col{0}; col < t->col_count(); ++col) {
    widths[col] = std::max(min, static_cast<uint16_t>(to_string(t->column_name(col)).size()));
  }

  // go over all rows and find the maximum length of the printed representation of a value, up to max
  for (ChunkID chunk_id{0}; chunk_id < _input_table_left()->chunk_count(); ++chunk_id) {
    auto& chunk = _input_table_left()->get_chunk(chunk_id);

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
