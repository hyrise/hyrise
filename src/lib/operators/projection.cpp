#include "projection.hpp"

#include "storage/reference_column.hpp"

namespace opossum {
Projection::Projection(const std::shared_ptr<AbstractOperator> in, const std::vector<std::string> &columns)
    : _table(in->get_output()), _column_filter(columns), _output(new Table) {}

const std::string Projection::get_name() const { return "Projection"; }

uint8_t Projection::get_num_in_tables() const { return 1; }

uint8_t Projection::get_num_out_tables() const { return 1; }

void Projection::execute() {
  for (auto column_name : _column_filter) {
    auto column_id = _table->get_column_id_by_name(column_name);
    auto ref_col = std::make_shared<ReferenceColumn>(_table, column_id, nullptr);
    _output->add_column(column_name, _table->get_column_type(column_id), false);
    _output->get_chunk(0).add_column(ref_col);
  }
}

std::shared_ptr<Table> Projection::get_output() const { return _output; }
}