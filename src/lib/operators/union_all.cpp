#include "union_all.hpp" // NEEDEDINCLUDE

#include "storage/table.hpp"

namespace opossum {
UnionAll::UnionAll(const std::shared_ptr<const AbstractOperator>& left_in,
                   const std::shared_ptr<const AbstractOperator>& right_in)
    : AbstractReadOnlyOperator(OperatorType::UnionAll, left_in, right_in) {
  // nothing to do here
}

const std::string UnionAll::name() const { return "UnionAll"; }

std::shared_ptr<const Table> UnionAll::_on_execute() {
  DebugAssert(input_table_left()->column_definitions() == input_table_right()->column_definitions(),
              "Input tables must have same number of columns");
  DebugAssert(input_table_left()->type() == input_table_left()->type(), "Input tables must have the same type");

  auto output = std::make_shared<Table>(input_table_left()->column_definitions(), input_table_left()->type());

  // add positions to output by iterating over both input tables
  for (const auto& input : {input_table_left(), input_table_right()}) {
    // iterating over all chunks of table input
    for (ChunkID in_chunk_id{0}; in_chunk_id < input->chunk_count(); in_chunk_id++) {
      // creating empty chunk to add segments with positions
      Segments output_segments;

      // iterating over all segments of the current chunk
      for (ColumnID column_id{0}; column_id < input->column_count(); ++column_id) {
        output_segments.push_back(input->get_chunk(in_chunk_id)->get_segment(column_id));
      }

      // adding newly filled chunk to the output table
      output->append_chunk(output_segments);
    }
  }

  return output;
}
std::shared_ptr<AbstractOperator> UnionAll::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<UnionAll>(copied_input_left, copied_input_right);
}

void UnionAll::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
