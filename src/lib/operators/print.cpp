#include "print.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>

#include "operators/table_wrapper.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/reference_segment.hpp"
#include "utils/performance_warning.hpp"

namespace {

using namespace hyrise;  // NOLINT

bool has_print_mvcc_flag(const PrintFlags flags) {
  return static_cast<uint32_t>(PrintFlags::Mvcc) & static_cast<uint32_t>(flags);
}

bool has_print_ignore_chunk_boundaries_flag(const PrintFlags flags) {
  return static_cast<uint32_t>(PrintFlags::IgnoreChunkBoundaries) & static_cast<uint32_t>(flags);
}

}  // namespace

namespace hyrise {

Print::Print(const std::shared_ptr<const AbstractOperator>& input_operator, const PrintFlags flags, std::ostream& out)
    : AbstractReadOnlyOperator(OperatorType::Print, input_operator), _flags(flags), _out(out) {}

const std::string& Print::name() const {
  static const auto name = std::string{"Print"};
  return name;
}

std::shared_ptr<AbstractOperator> Print::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Print>(copied_left_input, _flags, _out);
}

void Print::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void Print::print(const std::shared_ptr<const Table>& table, const PrintFlags flags, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, flags, out).execute();
}

void Print::print(const std::shared_ptr<const AbstractOperator>& op, const PrintFlags flags, std::ostream& out) {
  Print(op, flags, out).execute();
}

void Print::print(const std::string& sql, const PrintFlags flags, std::ostream& out) {
  auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
  const auto [status, result_tables] = pipeline.get_result_tables();
  Assert(status == SQLPipelineStatus::Success, "SQL execution was unsuccessful.");
  Assert(result_tables.size() == 1, "Expected exactly one result table.");
  Assert(result_tables.back(), "Unexpected null pointer.");

  auto table_wrapper = std::make_shared<TableWrapper>(result_tables[0]);
  table_wrapper->execute();

  Print(table_wrapper, flags, out).execute();
}

std::shared_ptr<const Table> Print::_on_execute() {
  auto performance_warning_disabler = PerformanceWarningDisabler{};

  const auto widths = _column_string_widths(_min_cell_width, _max_cell_width, left_input_table());
  const auto column_count = left_input_table()->column_count();

  // print column headers
  _out << "=== Columns" << std::endl;
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    _out << "|" << std::setw(widths[column_id]) << left_input_table()->column_name(column_id) << std::setw(0);
  }
  if (has_print_mvcc_flag(_flags)) {
    _out << "||        MVCC        ";
  }
  _out << "|" << std::endl;
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    _out << "|" << std::setw(widths[column_id]) << left_input_table()->column_data_type(column_id) << std::setw(0);
  }
  if (has_print_mvcc_flag(_flags)) {
    _out << "||_BEGIN|_END  |_TID  ";
  }
  _out << "|" << std::endl;
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto nullable = left_input_table()->column_is_nullable(column_id);
    _out << "|" << std::setw(widths[column_id]) << (nullable ? "null" : "not null") << std::setw(0);
  }
  if (has_print_mvcc_flag(_flags)) {
    _out << "||      |      |      ";
  }
  _out << "|" << std::endl;

  // print each chunk
  const auto chunk_count = left_input_table()->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = left_input_table()->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    if (!has_print_ignore_chunk_boundaries_flag(_flags)) {
      _out << "=== Chunk " << chunk_id << " ===" << std::endl;

      if (chunk->size() == 0) {
        _out << "Empty chunk." << std::endl;
        continue;
      }

      // print the encoding information
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto column_width = widths[column_id];
        const auto& segment = chunk->get_segment(column_id);
        _out << "|" << std::setw(column_width) << std::left << _segment_type(segment) << std::right << std::setw(0);
      }
      if (has_print_mvcc_flag(_flags)) {
        _out << "|";
      }
      _out << "|" << std::endl;
    }

    // print the rows in the chunk
    const auto chunk_size = chunk->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      _out << "|";
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        // well yes, we use AbstractSegment::operator[] here, but since Print is not an operation that should
        // be part of a regular query plan, let's keep things simple here
        auto column_width = widths[column_id];
        auto cell = _truncate_cell((*chunk->get_segment(column_id))[chunk_offset], column_width);
        _out << std::setw(column_width) << cell << "|" << std::setw(0);
      }

      if (has_print_mvcc_flag(_flags) && chunk->has_mvcc_data()) {
        auto mvcc_data = chunk->mvcc_data();

        auto begin = mvcc_data->get_begin_cid(chunk_offset);
        auto end = mvcc_data->get_end_cid(chunk_offset);
        auto tid = mvcc_data->get_tid(chunk_offset);

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

  return left_input_table();
}

// In order to print the table as an actual table, with columns being aligned, we need to calculate the
// number of characters in the printed representation of each column
// `min` and `max` can be used to limit the width of the columns - however, every column fits at least the column's name
std::vector<uint16_t> Print::_column_string_widths(uint16_t min, uint16_t max,
                                                   const std::shared_ptr<const Table>& table) const {
  const auto column_count = table->column_count();
  std::vector<uint16_t> widths(column_count);
  // calculate the length of the column name
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    widths[column_id] = std::max(min, static_cast<uint16_t>(table->column_name(column_id).size()));
  }

  // go over all rows and find the maximum length of the printed representation of a value, up to max
  const auto chunk_count = left_input_table()->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = left_input_table()->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      const auto chunk_size = chunk->size();
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        std::ostringstream stream;
        stream << (*chunk->get_segment(column_id))[chunk_offset];
        auto cell_length = static_cast<uint16_t>(stream.str().size());
        widths[column_id] = std::max({min, widths[column_id], std::min(max, cell_length)});
      }
    }
  }
  return widths;
}

std::string Print::_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) {
  auto cell_string = boost::lexical_cast<std::string>(cell);
  DebugAssert(max_width > 3, "Cannot truncate string with '...' at end with max_width <= 3");
  if (cell_string.length() > max_width) {
    return cell_string.substr(0, max_width - 3) + "...";
  }
  return cell_string;
}

std::string Print::_segment_type(const std::shared_ptr<AbstractSegment>& segment) {
  auto segment_type = std::string{};
  segment_type.reserve(10);
  segment_type += "<";

  if (std::dynamic_pointer_cast<BaseValueSegment>(segment)) {
    segment_type += "ValueS";
  } else if (std::dynamic_pointer_cast<ReferenceSegment>(segment)) {
    segment_type += "ReferS";
  } else if (const auto& encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment)) {
    switch (encoded_segment->encoding_type()) {
      case EncodingType::Unencoded: {
        Fail("An actual segment should never have this type");
      }
      case EncodingType::Dictionary: {
        segment_type += "Dic";
        break;
      }
      case EncodingType::RunLength: {
        segment_type += "RLE";
        break;
      }
      case EncodingType::FixedStringDictionary: {
        segment_type += "FSD";
        break;
      }
      case EncodingType::FrameOfReference: {
        segment_type += "FoR";
        break;
      }
      case EncodingType::LZ4: {
        segment_type += "LZ4";
        break;
      }
    }
    if (encoded_segment->compressed_vector_type()) {
      switch (*encoded_segment->compressed_vector_type()) {
        case CompressedVectorType::FixedWidthInteger4Byte: {
          segment_type += ":4B";
          break;
        }
        case CompressedVectorType::FixedWidthInteger2Byte: {
          segment_type += ":2B";
          break;
        }
        case CompressedVectorType::FixedWidthInteger1Byte: {
          segment_type += ":1B";
          break;
        }
        case CompressedVectorType::BitPacking: {
          segment_type += ":BitP";
          break;
        }
      }
    }
  } else {
    Fail("Unknown segment type");
  }
  segment_type += ">";
  return segment_type;
}

}  // namespace hyrise
