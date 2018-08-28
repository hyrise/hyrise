#include "print.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/base_segment.hpp"
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

  auto widths = _cxlumn_string_widths(_min_cell_width, _max_cell_width, input_table_left());

  // print cxlumn headers
  _out << "=== Cxlumns" << std::endl;
  for (CxlumnID cxlumn_id{0}; cxlumn_id < input_table_left()->cxlumn_count(); ++cxlumn_id) {
    _out << "|" << std::setw(widths[cxlumn_id]) << input_table_left()->cxlumn_name(cxlumn_id) << std::setw(0);
  }
  if (_flags & PrintMvcc) {
    _out << "||        MVCC        ";
  }
  _out << "|" << std::endl;
  for (CxlumnID cxlumn_id{0}; cxlumn_id < input_table_left()->cxlumn_count(); ++cxlumn_id) {
    const auto data_type = data_type_to_string.left.at(input_table_left()->cxlumn_data_type(cxlumn_id));
    _out << "|" << std::setw(widths[cxlumn_id]) << data_type << std::setw(0);
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
      for (CxlumnID cxlumn_id{0}; cxlumn_id < chunk->cxlumn_count(); ++cxlumn_id) {
        // well yes, we use BaseSegment::operator[] here, but since Print is not an operation that should
        // be part of a regular query plan, let's keep things simple here
        auto cxlumn_width = widths[cxlumn_id];
        auto cell = _truncate_cell((*chunk->get_segment(cxlumn_id))[row], cxlumn_width);
        _out << std::setw(cxlumn_width) << cell << "|" << std::setw(0);
      }

      if (_flags & PrintMvcc && chunk->has_mvcc_data()) {
        auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

        auto begin = mvcc_data->begin_cids[row];
        auto end = mvcc_data->end_cids[row];
        auto tid = mvcc_data->tids[row];

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

// In order to print the table as an actual table, with cxlumns being aligned, we need to calculate the
// number of characters in the printed representation of each cxlumn
// `min` and `max` can be used to limit the width of the cxlumns - however, every cxlumn fits at least the cxlumn's name
std::vector<uint16_t> Print::_cxlumn_string_widths(uint16_t min, uint16_t max,
                                                   const std::shared_ptr<const Table>& table) const {
  std::vector<uint16_t> widths(table->cxlumn_count());
  // calculate the length of the cxlumn name
  for (CxlumnID cxlumn_id{0}; cxlumn_id < table->cxlumn_count(); ++cxlumn_id) {
    widths[cxlumn_id] = std::max(min, static_cast<uint16_t>(table->cxlumn_name(cxlumn_id).size()));
  }

  // go over all rows and find the maximum length of the printed representation of a value, up to max
  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    auto chunk = input_table_left()->get_chunk(chunk_id);

    for (CxlumnID cxlumn_id{0}; cxlumn_id < chunk->cxlumn_count(); ++cxlumn_id) {
      for (size_t row = 0; row < chunk->size(); ++row) {
        auto cell_length = static_cast<uint16_t>(to_string((*chunk->get_segment(cxlumn_id))[row]).size());
        widths[cxlumn_id] = std::max({min, widths[cxlumn_id], std::min(max, cell_length)});
      }
    }
  }
  return widths;
}

std::string Print::_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) const {
  auto cell_string = type_cast<std::string>(cell);
  DebugAssert(max_width > 3, "Cannot truncate string with '...' at end with max_width <= 3");
  if (cell_string.length() > max_width) {
    return cell_string.substr(0, max_width - 3) + "...";
  }
  return cell_string;
}

}  // namespace opossum
