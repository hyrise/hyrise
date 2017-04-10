#include "sort.hpp"

#include <memory>
#include <string>

namespace opossum {
Sort::Sort(const std::shared_ptr<const AbstractOperator> in, const std::string &sort_column_name, const bool ascending,
           const size_t output_chunk_size)
    : AbstractReadOnlyOperator(in),
      _sort_column_name(sort_column_name),
      _ascending(ascending),
      _output_chunk_size(output_chunk_size) {}

const std::string Sort::name() const { return "Sort"; }

uint8_t Sort::num_in_tables() const { return 1; }

uint8_t Sort::num_out_tables() const { return 1; }

std::shared_ptr<const Table> Sort::on_execute() {
  _impl = make_unique_by_column_type<AbstractReadOnlyOperatorImpl, SortImpl>(
      input_table_left()->column_type(input_table_left()->column_id_by_name(_sort_column_name)), input_table_left(),
      _sort_column_name, _ascending, _output_chunk_size);
  return _impl->on_execute();
}

}  // namespace opossum
