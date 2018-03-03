#include "case.hpp"

#include "storage/column_iterables/any_column_iterator.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/materialize.hpp"

namespace opossum {

Case::Case(const std::shared_ptr<AbstractOperator>& input, std::vector<std::unique_ptr<AbstractCaseExpression>> case_expressions):
  AbstractReadOnlyOperator(input), _case_expressions(std::move(case_expressions))
{}

const std::string Case::name() const {
  return "Case";
}

std::shared_ptr<const Table> Case::_on_execute() {
  const auto output_table = Table::create_with_layout_from(_input_table_left());

  for (ChunkID chunk_id{0}; chunk_id < _input_table_left()->chunk_count(); ++chunk_id) {
    const auto input_chunk = _input_table_left()->get_chunk(chunk_id);

    output_table->emplace_chunk(input_chunk->forward_data_or_materialize());

    std::shared_ptr<Chunk> output_chunk;
    if (_input_table_left()->get_type() == TableType::References) {
//      for (ColumnID column_id{0}; column_id < _input_table_left()->column_count(); ++column_id) {
//        resolve_data_and_column_type(_input_table_left()->column_type(column_id), *input_chunk->get_column(column_id), [&](auto type, const auto& column) {
//
//        });
//      }
      output_chunk = input_chunk->materialized();
    } else {
//      for (ColumnID column_id{0}; column_id < _input_table_left()->column_count(); ++column_id) {
//        output_chunk->add_column(input_chunk->get_column(column_id));
//      }
      output_chunk = input_chunk->forwarded();
    }

    std::vector<std::optional<std::vector<int32_t>>> materialized_when_columns(_input_table_left()->column_count());

    for (const auto &case_expression : _case_expressions) {
      resolve_data_type(case_expression->result_data_type, [&](auto type) {
        using ResultDataType = typename decltype(type)::type;

        std::vector<std::optional<std::vector<std::pair<bool, ResultDataType>>>> materialized_then_columns(_input_table_left()->column_count());

        const auto& typed_case_expression = static_cast<PhysicalCaseExpression<ResultDataType>&>(*case_expression);

        pmr_concurrent_vector<ResultDataType> output_values(input_chunk->size());
        pmr_concurrent_vector<bool> output_nulls(input_chunk->size());

        for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
          for (const auto &case_clause : typed_case_expression.clauses) {
            auto &materialized_when_column = materialized_when_columns[case_clause.when];
            if (!materialized_when_column) {
              materialized_when_column.emplace();
              materialized_when_column->reserve(input_chunk->size());
              materialize_values(*input_chunk->get_column(case_clause.when), *materialized_when_column);
            }

            if ((*materialized_when_column)[chunk_offset] == int32_t{0}) continue;

            if (case_clause.then.type() == typeid(Null)) {
              output_values[chunk_offset] = ResultDataType{};
              output_nulls[chunk_offset] = true;
            } else if (case_clause.then.type() == typeid(ColumnID)) {
              const auto then_column_id = boost::get<ColumnID>(case_clause.then);
              auto &materialized_then_column = materialized_then_columns[then_column_id];
              if (!materialized_then_column) {
                materialized_then_column.emplace();
                materialized_then_column->reserve(input_chunk->size());
                materialize_values_and_nulls(*input_chunk->get_column(then_column_id), *materialized_then_column);
              }

              // Create alias to hopefully ensured this isn't done twice
              const auto& materialized_then_value_and_null = (*materialized_then_column)[chunk_offset];
              output_values[chunk_offset] = materialized_then_value_and_null.second;
              output_nulls[chunk_offset] = materialized_then_value_and_null.first;
            } else if (case_clause.then.type() == typeid(ResultDataType)) {
              output_values[chunk_offset] = boost::get<ResultDataType>(case_clause.then);
              output_nulls[chunk_offset] = false;
            } else {
              Fail("Unexpected type in THEN");
            }
          }
        }

        output_chunk->add_column(std::make_shared<ValueColumn<ResultDataType>>(output_values, output_nulls));
      });
    }
  }

  return output_table;
}

}  // namespace opossum
