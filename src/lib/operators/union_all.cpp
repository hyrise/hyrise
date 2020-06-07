#include "union_all.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {
UnionAll::UnionAll(const std::shared_ptr<const AbstractOperator>& left_in,
                   const std::shared_ptr<const AbstractOperator>& right_in)
    : AbstractReadOnlyOperator(OperatorType::UnionAll, left_in, right_in) {
  // nothing to do here
}

const std::string& UnionAll::name() const {
  static const auto name = std::string{"UnionAll"};
  return name;
}

std::shared_ptr<const Table> UnionAll::_on_execute() {
  DebugAssert(left_input_table()->column_definitions() == right_input_table()->column_definitions(),
              "Input tables must have same number of columns");
  DebugAssert(left_input_table()->type() == right_input_table()->type(), "Input tables must have the same type");

  auto output_chunks =
      std::vector<std::shared_ptr<Chunk>>{left_input_table()->chunk_count() + right_input_table()->chunk_count()};
  auto output_chunk_idx = size_t{0};

  // add positions to output by iterating over both input tables
  for (const auto& input : {left_input_table(), right_input_table()}) {
    // iterating over all chunks of table input
    const auto chunk_count = input->chunk_count();
    for (ChunkID in_chunk_id{0}; in_chunk_id < chunk_count; in_chunk_id++) {
      const auto chunk = input->get_chunk(in_chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      // creating empty chunk to add segments with positions
      Segments output_segments;

      // iterating over all segments of the current chunk
      for (ColumnID column_id{0}; column_id < input->column_count(); ++column_id) {
        output_segments.push_back(chunk->get_segment(column_id));
      }

      // adding newly filled chunk to the output table
      output_chunks[output_chunk_idx] = std::make_shared<Chunk>(output_segments);
      ++output_chunk_idx;
    }
  }

  return std::make_shared<Table>(left_input_table()->column_definitions(), left_input_table()->type(),
                                 std::move(output_chunks));
}
std::shared_ptr<AbstractOperator> UnionAll::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<UnionAll>(copied_left_input, copied_right_input);
}

void UnionAll::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
