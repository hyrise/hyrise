#include <initializer_list>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "storage/reference_column.hpp"
#include "union_all.hpp"

namespace opossum {
UnionAll::UnionAll(const std::shared_ptr<const AbstractOperator> left_in,
                   const std::shared_ptr<const AbstractOperator> right_in)
    : AbstractOperator(left_in, right_in), _output(std::make_shared<Table>()) {
  // nothing to do here
}

const std::string UnionAll::name() const { return "UnionAll"; }

uint8_t UnionAll::num_in_tables() const { return 2; }

uint8_t UnionAll::num_out_tables() const { return 1; }

void UnionAll::execute() {
  if (_input_left->col_count() != _input_right->col_count()) {
    throw std::runtime_error("Input tables must have same number of columns");
  }

  // copy column definition from _input_left to output table
  for (size_t column_id = 0; column_id < _input_left->col_count(); ++column_id) {
    auto column_type = _input_left->column_type(column_id);
    if (column_type != _input_right->column_type(column_id)) {
      throw std::runtime_error("Input tables must have same column order and column types");
    }
    // add column definition to output table
    _output->add_column(_input_left->column_name(column_id), column_type, false);
  }

  // add positions to output by iterating over both input tables
  for (const auto &input : (const std::shared_ptr<const Table>[]){_input_left, _input_right}) {
    // iterating over all chunks of table input
    for (ChunkID in_chunk_id = 0; in_chunk_id < input->chunk_count(); in_chunk_id++) {
      // creating empty chunk to add columns with positions
      Chunk chunk_output;

      // iterating over all columns of the current chunk
      for (size_t column_id = 0; column_id < input->col_count(); ++column_id) {
        chunk_output.add_column(input->get_chunk(in_chunk_id).get_column(column_id));
      }

      // adding newly filled chunk to the output table
      _output->add_chunk(std::move(chunk_output));
    }
  }
}

std::shared_ptr<const Table> UnionAll::get_output() const { return _output; }

}  // namespace opossum
