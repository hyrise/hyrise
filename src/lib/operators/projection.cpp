#include <memory>
#include <string>
#include <vector>

#include "projection.hpp"
#include "storage/reference_column.hpp"

namespace opossum {
Projection::Projection(const std::shared_ptr<AbstractOperator> in, const std::vector<std::string> &columns)
    : AbstractOperator(in), _column_filter(columns), _output(new Table) {}

const std::string Projection::get_name() const { return "Projection"; }

uint8_t Projection::get_num_in_tables() const { return 1; }

uint8_t Projection::get_num_out_tables() const { return 1; }

void Projection::execute() {
  for (auto column_name : _column_filter) {
    auto column_id = _input_left->get_column_id_by_name(column_name);
    std::shared_ptr<ReferenceColumn> ref_col;
    if (auto referenced_col =
            std::dynamic_pointer_cast<ReferenceColumn>(_input_left->get_chunk(0).get_column(column_id))) {
      ref_col = std::make_shared<ReferenceColumn>(referenced_col->get_referenced_table(), column_id, nullptr);
    } else {
      ref_col = std::make_shared<ReferenceColumn>(_input_left, column_id, nullptr);
    }
    _output->add_column(column_name, _input_left->get_column_type(column_id), false);
    _output->get_chunk(0).add_column(ref_col);
  }
}

std::shared_ptr<Table> Projection::get_output() const { return _output; }
}  // namespace opossum
