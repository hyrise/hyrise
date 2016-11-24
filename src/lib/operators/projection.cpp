#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "projection.hpp"
#include "storage/reference_column.hpp"

namespace opossum {
Projection::Projection(const std::shared_ptr<const AbstractOperator> in, const std::vector<std::string>& columns)
    : AbstractOperator(in), _column_filter(columns), _output(std::make_shared<Table>()) {}

const std::string Projection::name() const { return "Projection"; }

uint8_t Projection::num_in_tables() const { return 1; }

uint8_t Projection::num_out_tables() const { return 1; }

void Projection::execute() {
  // stores the ids of the columns that are part of the result table (in the correct order)
  std::vector<size_t> column_ids;
  column_ids.reserve(_column_filter.size());

  // fill column_ids vector and add columns to table schema
  for (const auto& column_name : _column_filter) {
    auto column_id = _input_left->column_id_by_name(column_name);
    column_ids.push_back(column_id);
    _output->add_column(column_name, _input_left->column_type(column_id), false);
  }

  // for each chunk, copy shared_ptr to column in input table
  for (ChunkID chunk_id = 0; chunk_id < _input_left->chunk_count(); chunk_id++) {
    const Chunk& chunk_in = _input_left->get_chunk(chunk_id);
    Chunk chunk_out;

    for (auto column_id : column_ids) {
      chunk_out.add_column(chunk_in.get_column(column_id));
    }

    _output->add_chunk(std::move(chunk_out));
  }
}

std::shared_ptr<const Table> Projection::get_output() const { return _output; }
}  // namespace opossum
