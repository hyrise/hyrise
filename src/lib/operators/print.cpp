#include "print.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/base_column.hpp"
#include "type_cast.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

Print::Print(const std::shared_ptr<const AbstractOperator>& in, std::ostream& out, uint32_t flags)
    : AbstractReadOnlyOperator(OperatorType::Print, in), _out(out), _flags(flags) {}

const std::string Print::name() const { return "Print"; }

std::shared_ptr<AbstractOperator> Print::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Print>(copied_input_left, _out);
}

void Print::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void Print::print(const std::shared_ptr<const Table>& table, uint32_t flags, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, out, flags).execute();
}

void Print::print(const std::shared_ptr<const AbstractOperator>& in, uint32_t flags, std::ostream& out) {
  Print(in, out, flags).execute();
}

std::shared_ptr<const Table> Print::_on_execute() {
  PerformanceWarningDisabler pwd;

  auto widths = _column_string_widths(8, 20, input_table_left());

  // print column headers
  _out << "=== Columns" << std::endl;
  for (ColumnID col{0}; col < input_table_left()->column_count(); ++col) {
    _out << "|" << std::setw(widths[col]) << input_table_left()->column_name(col) << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||        MVCC        ";
  }
  _out << "|" << std::endl;
  for (ColumnID col{0}; col < input_table_left()->column_count(); ++col) {
    const auto data_type = data_type_to_string.left.at(input_table_left()->column_data_type(col));
    _out << "|" << std::setw(widths[col]) << data_type << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||_BEGIN|_END  |_TID  ";
  }
  _out << "|" << std::endl;

  // print each chunk
  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    auto chunk = input_table_left()->get_chunk(chunk_id);
    if (chunk->size() == 0 && (_flags & PrintIgnoreEmptyChunks)) {
      continue;
    }

    _out << "=== Chunk " << chunk_id << " === " << std::endl;

    if (chunk->size() == 0) {
      _out << "Empty chunk." << std::endl;
      continue;
    }

    // print the rows in the chunk
    for (size_t row = 0; row < chunk->size(); ++row) {
      _out << "|";
      for (ColumnID col{0}; col < chunk->column_count(); ++col) {
        // well yes, we use BaseColumn::operator[] here, but since Print is not an operation that should
        // be part of a regular query plan, let's keep things simple here
        auto col_width = widths[col];
        auto cell = _truncate_cell((*chunk->get_column(col))[row], col_width);
        _out << std::setw(col_width) << cell << "|" << std::setw(0);
      }

      if (_flags & PrintMvcc && chunk->has_mvcc_columns()) {
        auto mvcc_columns = chunk->get_scoped_mvcc_columns_lock();

        auto begin = mvcc_columns->begin_cids[row];
        auto end = mvcc_columns->end_cids[row];
        auto tid = mvcc_columns->tids[row];

        auto begin_str = begin == MvccColumns::MAX_COMMIT_ID ? "" : std::to_string(begin);
        auto end_str = end == MvccColumns::MAX_COMMIT_ID ? "" : std::to_string(end);
        auto tid_str = tid == 0 ? "" : std::to_string(tid);

        _out << "|" << std::setw(6) << begin_str << std::setw(0);
        _out << "|" << std::setw(6) << end_str << std::setw(0);
        _out << "|" << std::setw(6) << tid_str << std::setw(0);
        _out << "|";
      }
      _out << std::endl;
    }
  }

  return input_table_left();
}

// In order to print the table as an actual table, with columns being aligned, we need to calculate the
// number of characters in the printed representation of each column
// `min` and `max` can be used to limit the width of the columns - however, every column fits at least the column's name
std::vector<uint16_t> Print::_column_string_widths(uint16_t min, uint16_t max,
                                                   const std::shared_ptr<const Table>& table) const {
  std::vector<uint16_t> widths(table->column_count());
  // calculate the length of the column name
  for (ColumnID col{0}; col < table->column_count(); ++col) {
    widths[col] = std::max(min, static_cast<uint16_t>(table->column_name(col).size()));
  }

  // go over all rows and find the maximum length of the printed representation of a value, up to max
  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    auto chunk = input_table_left()->get_chunk(chunk_id);

    for (ColumnID col{0}; col < chunk->column_count(); ++col) {
      for (size_t row = 0; row < chunk->size(); ++row) {
        auto cell_length = static_cast<uint16_t>(to_string((*chunk->get_column(col))[row]).size());
        widths[col] = std::max({min, widths[col], std::min(max, cell_length)});
      }
    }
  }
  return widths;
}

std::string Print::_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) const {
  auto cell_str = type_cast<std::string>(cell);
  DebugAssert(max_width > 3, "Cannot truncate string with '...' at end with max_width <= 3");
  if (cell_str.length() > max_width) {
    return cell_str.substr(0, max_width - 3) + "...";
  }
  return cell_str;
}

}  // namespace opossum
