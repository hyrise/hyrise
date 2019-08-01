#include "difference.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {
Difference::Difference(const std::shared_ptr<const AbstractOperator>& left_in,
                       const std::shared_ptr<const AbstractOperator>& right_in)
    : AbstractReadOnlyOperator(OperatorType::Difference, left_in, right_in) {}

const std::string Difference::name() const { return "Difference"; }

std::shared_ptr<AbstractOperator> Difference::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Difference>(copied_input_left, copied_input_right);
}

void Difference::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Difference::_on_execute() {
  DebugAssert(input_table_left()->column_definitions() == input_table_right()->column_definitions(),
              "Input tables must have same number of columns");

  // 1. We create a set of all right input rows as concatenated strings.

  auto right_input_row_set = std::unordered_set<std::string>(input_table_right()->row_count());

  // Iterating over all chunks and for each chunk over all segments
  const auto chunk_count_right = input_table_right()->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count_right; chunk_id++) {
    const auto chunk = input_table_right()->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    // creating a temporary row representation with strings to be filled segment-wise
    auto string_row_vector = std::vector<std::stringstream>(chunk->size());
    for (ColumnID column_id{0}; column_id < input_table_right()->column_count(); column_id++) {
      const auto base_segment = chunk->get_segment(column_id);

      // filling the row vector with all values from this segment
      auto row_string_buffer = std::stringstream{};
      for (ChunkOffset chunk_offset = 0; chunk_offset < base_segment->size(); chunk_offset++) {
        // Previously we called a virtual method of the BaseSegment interface here.
        // It was replaced with a call to the subscript operator as that is equally slow.
        const auto value = (*base_segment)[chunk_offset];
        _append_string_representation(string_row_vector[chunk_offset], value);
      }
    }

    // Remove duplicate rows by adding all rows to a unordered set
    std::transform(string_row_vector.cbegin(), string_row_vector.cend(),
                   std::inserter(right_input_row_set, right_input_row_set.end()), [](auto& x) { return x.str(); });
  }

  // 2. Now we check for each chunk of the left input which rows can be added to the output

  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(input_table_left()->chunk_count());

  // Iterating over all chunks and for each chunk over all segment
  const auto chunk_count_left = input_table_left()->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count_left; chunk_id++) {
    const auto in_chunk = input_table_left()->get_chunk(chunk_id);
    Assert(in_chunk, "Did not expect deleted chunk here.");  // see #1686

    Segments output_segments;

    // creating a map to share pos_lists (see table_scan.hpp)
    std::unordered_map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> out_pos_list_map;

    for (ColumnID column_id{0}; column_id < input_table_left()->column_count(); column_id++) {
      const auto base_segment = in_chunk->get_segment(column_id);
      // temporary variables needed to create the reference segment
      const auto referenced_segment =
          std::dynamic_pointer_cast<const ReferenceSegment>(in_chunk->get_segment(column_id));
      auto out_column_id = column_id;
      auto out_referenced_table = input_table_left();
      std::shared_ptr<const PosList> in_pos_list;

      if (referenced_segment) {
        // if the input segment was a reference segment then the output segment must reference the same values/objects
        out_column_id = referenced_segment->referenced_column_id();
        out_referenced_table = referenced_segment->referenced_table();
        in_pos_list = referenced_segment->pos_list();
      }

      // automatically creates the entry if it does not exist
      std::shared_ptr<PosList>& pos_list_out = out_pos_list_map[in_pos_list];

      if (!pos_list_out) {
        pos_list_out = std::make_shared<PosList>();
      }

      // creating a ReferenceSegment for the output
      auto out_reference_segment =
          std::make_shared<ReferenceSegment>(out_referenced_table, out_column_id, pos_list_out);
      output_segments.push_back(out_reference_segment);
    }

    // for all offsets check if the row can be added to the output
    for (ChunkOffset chunk_offset = 0; chunk_offset < in_chunk->size(); chunk_offset++) {
      // creating string representation off the row at chunk_offset
      auto row_string_buffer = std::stringstream{};
      for (ColumnID column_id{0}; column_id < input_table_left()->column_count(); column_id++) {
        const auto base_segment = in_chunk->get_segment(column_id);

        // Previously a virtual method of the BaseSegment interface was called here.
        // It was replaced with a call to the subscript operator as that is equally slow.
        const auto value = (*base_segment)[chunk_offset];
        _append_string_representation(row_string_buffer, value);
      }
      const auto row_string = row_string_buffer.str();

      // we check if the recently created row_string is contained in the left_input_row_set
      auto search = right_input_row_set.find(row_string);
      if (search == right_input_row_set.end()) {
        for (const auto& pos_list_pair : out_pos_list_map) {
          if (pos_list_pair.first) {
            pos_list_pair.second->emplace_back((*pos_list_pair.first)[chunk_offset]);
          } else {
            pos_list_pair.second->emplace_back(RowID{chunk_id, chunk_offset});
          }
        }
      }
    }

    // Only add chunk if it would contain any tuples
    if (!output_segments.empty() && output_segments[0]->size() > 0) {
      output_chunks.emplace_back(std::make_shared<Chunk>(output_segments));
    }
  }

  return std::make_shared<Table>(input_table_left()->column_definitions(), TableType::References,
                                 std::move(output_chunks));
}

void Difference::_append_string_representation(std::ostream& row_string_buffer, const AllTypeVariant& value) {
  const auto string_value = boost::lexical_cast<std::string>(value);
  const auto length = static_cast<uint32_t>(string_value.length());

  // write value as string
  row_string_buffer << string_value;

  // write byte representation of length
  row_string_buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));
}

}  // namespace opossum
