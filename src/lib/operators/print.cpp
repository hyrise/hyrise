#include "print.hpp"  // NEEDEDINCLUDE

#include <iomanip>  // NEEDEDINCLUDE
#include <iostream>

#include "operators/table_wrapper.hpp"  // NEEDEDINCLUDE
#include "storage/mvcc_data.hpp"
#include "storage/table.hpp"

namespace opossum {

Print::Print(const std::shared_ptr<const AbstractOperator>& in, uint32_t flags)
    : AbstractReadOnlyOperator(OperatorType::Print, in), _out(std::cout), _flags(flags) {}

Print::Print(const std::shared_ptr<const AbstractOperator>& in, uint32_t flags, std::ostream& out)
    : AbstractReadOnlyOperator(OperatorType::Print, in), _out(out), _flags(flags) {}

const std::string Print::name() const { return "Print"; }

std::shared_ptr<AbstractOperator> Print::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Print>(copied_input_left, _out);
}

void Print::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void Print::print(const std::shared_ptr<const Table>& table, uint32_t flags) {
  print(table, flags, std::cout);
}

void Print::print(const std::shared_ptr<const Table>& table, uint32_t flags, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, out, flags).execute();
}

void Print::print(const std::shared_ptr<const AbstractOperator>& in, uint32_t flags) {
  print(in, flags, std::cout);
}

void Print::print(const std::shared_ptr<const AbstractOperator>& in, uint32_t flags, std::ostream& out) {
  Print(in, out, flags).execute();
}

std::shared_ptr<const Table> Print::_on_execute() {
  PerformanceWarningDisabler pwd;

  auto widths = _column_string_widths(_min_cell_width, _max_cell_width, input_table_left());

  // print column headers
  _out << "=== Columns" << std::endl;
  for (ColumnID column_id{0}; column_id < input_table_left()->column_count(); ++column_id) {
    _out << "|" << std::setw(widths[column_id]) << input_table_left()->column_name(column_id) << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||        MVCC        ";
  }
  _out << "|" << std::endl;
  for (ColumnID column_id{0}; column_id < input_table_left()->column_count(); ++column_id) {
    const auto data_type = data_type_to_string.left.at(input_table_left()->column_data_type(column_id));
    _out << "|" << std::setw(widths[column_id]) << data_type << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||_BEGIN|_END  |_TID  ";
  }
  _out << "|" << std::endl;
  for (ColumnID column_id{0}; column_id < input_table_left()->column_count(); ++column_id) {
    const auto nullable = input_table_left()->column_is_nullable(column_id);
    _out << "|" << std::setw(widths[column_id]) << (nullable ? "null" : "not null") << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||      |      |      ";
  }
  _out << "|" << std::endl;

  // print each chunk
  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    auto chunk = input_table_left()->get_chunk(chunk_id);
    if (chunk->size() == 0 && (_flags & PrintIgnoreEmptyChunks)) {
      continue;
    }

    _out << "=== Chunk " << chunk_id << " ===" << std::endl;

    if (chunk->size() == 0) {
      _out << "Empty chunk." << std::endl;
      continue;
    }

    // print the rows in the chunk
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk->size(); ++chunk_offset) {
      _out << "|";
      for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
        // well yes, we use BaseSegment::operator[] here, but since Print is not an operation that should
        // be part of a regular query plan, let's keep things simple here
        auto column_width = widths[column_id];
        auto cell = _truncate_cell((*chunk->get_segment(column_id))[chunk_offset], column_width);
        _out << std::setw(column_width) << cell << "|" << std::setw(0);
      }

      if (_flags & PrintMvcc && chunk->has_mvcc_data()) {
        auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

        auto begin = mvcc_data->begin_cids[chunk_offset];
        auto end = mvcc_data->end_cids[chunk_offset];
        auto tid = mvcc_data->tids[chunk_offset];

        auto begin_string = begin == MvccData::MAX_COMMIT_ID ? "" : std::to_string(begin);
        auto end_string = end == MvccData::MAX_COMMIT_ID ? "" : std::to_string(end);
        auto tid_string = tid == 0 ? "" : std::to_string(tid);

        _out << "|" << std::setw(6) << begin_string << std::setw(0);
        _out << "|" << std::setw(6) << end_string << std::setw(0);
        _out << "|" << std::setw(6) << tid_string << std::setw(0);
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
  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    widths[column_id] = std::max(min, static_cast<uint16_t>(table->column_name(column_id).size()));
  }

  // go over all rows and find the maximum length of the printed representation of a value, up to max
  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    auto chunk = input_table_left()->get_chunk(chunk_id);

    for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk->size(); ++chunk_offset) {
        auto cell_length = static_cast<uint16_t>(to_string((*chunk->get_segment(column_id))[chunk_offset]).size());
        widths[column_id] = std::max({min, widths[column_id], std::min(max, cell_length)});
      }
    }
  }
  return widths;
}

std::string Print::_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) const {
  // Use lexical_cast instead of type_cast here so that floats get truncated
  auto cell_string = boost::lexical_cast<std::string>(cell);
  DebugAssert(max_width > 3, "Cannot truncate string with '...' at end with max_width <= 3");
  if (cell_string.length() > max_width) {
    return cell_string.substr(0, max_width - 3) + "...";
  }
  return cell_string;
}

}  // namespace opossum
