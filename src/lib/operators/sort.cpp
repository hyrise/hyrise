#include "sort.hpp"

namespace opossum {
Sort::Sort(const std::shared_ptr<AbstractOperator> in, const std::string &sort_column_name, const bool ascending)
    : AbstractOperator(in),
      _impl(make_unique_by_column_type<AbstractOperatorImpl, SortImpl>(
          _input_left->get_column_type(_input_left->get_column_id_by_name(sort_column_name)), in, sort_column_name,
          ascending)) {}
const std::string Sort::get_name() const { return "Sort"; }

uint8_t Sort::get_num_in_tables() const { return 1; }

uint8_t Sort::get_num_out_tables() const { return 1; }

void Sort::execute() { _impl->execute(); }

std::shared_ptr<Table> Sort::get_output() const { return _impl->get_output(); }
}