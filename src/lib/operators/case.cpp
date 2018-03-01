#include "case.hpp"

#include "storage/column_iterables/any_column_iterator.hpp"
#include "storage/create_iterable_from_column.hpp"

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

    for (const auto &case_expression : _case_expressions) {
      resolve_data_type(case_expression->result_data_type, [&](auto type) {
        using ResultDataType = typename decltype(type)::type;

        const auto& typed_case_expression = static_cast<PhysicalCaseExpression<ResultDataType>&>(*case_expression);

        struct ClauseIterator {
          AnyColumnIterator<int32_t> when_iterator; // When-Columns need to be int32_t
          boost::variant<Null, ResultDataType, AnyColumnIterator<ResultDataType>> then_iterator_or_value;

          std::optional<ResultDataType> then_value() const {
            if (then_iterator_or_value.type() == typeid(Null)) {
              return std::nullopt;
            } else if (then_iterator_or_value.type() == ResultDataType) {
              return boost::get<ResultDataType>(then_iterator_or_value)
            } else if (then_iterator_or_value.type() == typeid(AnyColumnIterator<ResultDataType>)) {
              return *boost::get<AnyColumnIterator<ResultDataType>>(then_iterator_or_value);
            } else {
              Fail("Unexpected type in THEN");
            }
          }
        };

        std::vector<ClauseIterator> clause_iterators;

        for (const auto& case_clause : typed_case_expression.clauses) {
          ClauseIterator clause_iterator;

          const auto when_base_column = input_chunk->get_column(case_clause.when);
          resolve_column_type<int32_t>(*when_base_column, [&](const auto& when_column) { // When-Columns need to be int32_t
            const auto iterable = erase_type_from_iterable(create_iterable_from_column(when_column));
            clause_iterator.when_iterator = iterable.begin();
          });

          if (case_clause.then.type() == typeid(Null)) {
            clause_iterator.then_iterator_or_value = Null{};
          } else if (case_clause.then.type() == typeid(ColumnID)) {
            const auto then_base_column = input_chunk->get_column(boost::get<ColumnID>(case_clause.then));
            resolve_column_type<ResultDataType>(*then_base_column, [&](const auto& then_column) {
              const auto iterable = erase_type_from_iterable(create_iterable_from_column(then_column));
              clause_iterator.then_iterator_or_value = iterable.begin();
            });
          } else if (case_clause.then.type() == typeid(ResultDataType)) {
            clause_iterator.then_iterator_or_value = boost::get<ResultDataType>(case_clause.then);
          } else {
            Fail("Unexpected type in THEN");
          }

          clause_iterators.emplace_back(clause_iterator);
        }

        pmr_concurrent_vector<ResultDataType> output_values(input_chunk->size());
        pmr_concurrent_vector<bool> output_nulls(input_chunk->size());

        for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
          for (const auto& clause_iterator : clause_iterators) {
            if (*clause_iterator.when_iterator != int32_t{0}) {
              const auto then_value = clause_iterator.then_value();
              output_values.emplace_back(then_value ? *then_value : ResulDataType{});
              output_nulls.emplace_back(static_cast<bool>(then_value));
            }

            clause_iterator.when_iterator.increment();
            if (clause_iterator.then_iterator_or_value.type() == typeid(AnyColumnIterator<ResultDataType>)) {
              boost::get<AnyColumnIterator<ResultDataType>>(clause_iterator.then_iterator_or_value).increment();
            }
          }
        }
      });
    }
  }

  return output_table;
}

}  // namespace opossum
