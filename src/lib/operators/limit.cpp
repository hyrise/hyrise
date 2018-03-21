#include "limit.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/reference_column.hpp"
#include "storage/table.hpp"

namespace opossum {

Limit::Limit(const std::shared_ptr<const AbstractOperator> in, const size_t num_rows)
    : AbstractReadOnlyOperator(OperatorType::Limit, in), _num_rows(num_rows) {}

const std::string Limit::name() const { return "Limit"; }

std::shared_ptr<AbstractOperator> Limit::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<Limit>(recreated_input_left, _num_rows);
}

size_t Limit::num_rows() const { return _num_rows; }

std::shared_ptr<const Table> Limit::_on_execute() {
  const auto input_table = input_table_left();

  auto output_table = std::make_shared<Table>(input_table->column_definitions(), TableType::References);

  ChunkID chunk_id{0};
  for (size_t i = 0; i < _num_rows && chunk_id < input_table->chunk_count(); chunk_id++) {
    const auto input_chunk = input_table->get_chunk(chunk_id);
    ChunkColumns output_columns;

    size_t output_chunk_row_count = std::min<size_t>(input_chunk->size(), _num_rows - i);

    for (ColumnID column_id{0}; column_id < input_table->column_count(); column_id++) {
      const auto input_base_column = input_chunk->get_column(column_id);
      auto output_pos_list = std::make_shared<PosList>(output_chunk_row_count);
      std::shared_ptr<const Table> referenced_table;
      ColumnID output_column_id = column_id;

      if (auto input_ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(input_base_column)) {
        output_column_id = input_ref_column->referenced_column_id();
        referenced_table = input_ref_column->referenced_table();
        // TODO(all): optimize using whole chunk whenever possible
        auto begin = input_ref_column->pos_list()->begin();
        std::copy(begin, begin + output_chunk_row_count, output_pos_list->begin());
      } else {
        referenced_table = input_table;
        for (ChunkOffset chunk_offset = 0; chunk_offset < output_chunk_row_count; chunk_offset++) {
          (*output_pos_list)[chunk_offset] = RowID{chunk_id, chunk_offset};
        }
      }

      output_columns.push_back(std::make_shared<ReferenceColumn>(referenced_table, output_column_id, output_pos_list));
    }

    i += output_chunk_row_count;
    output_table->append_chunk(output_columns);
  }

  return output_table;
}

}  // namespace opossum
