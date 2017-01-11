#include "sort.hpp"

#include <memory>
#include <string>

namespace opossum {
Sort::Sort(const std::shared_ptr<const AbstractOperator> in, const std::string &sort_column_name, const bool ascending)
    : AbstractOperator(in), _sort_column_name(sort_column_name), _ascending(ascending) {}

const std::string Sort::name() const { return "Sort"; }

uint8_t Sort::num_in_tables() const { return 1; }

uint8_t Sort::num_out_tables() const { return 1; }

std::shared_ptr<const Table> Sort::on_execute() {
  _impl = make_unique_by_column_type<AbstractOperatorImpl, SortImpl>(
      input_table_left()->column_type(input_table_left()->column_id_by_name(_sort_column_name)), _input_left,
      _sort_column_name, _ascending);
  return _impl->on_execute();
}

}  // namespace opossum
