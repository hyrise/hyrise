#include "aggregate_sort.hpp"

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "operators/sort.hpp"

namespace opossum {

    AggregateSort::AggregateSort(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                  const std::vector<ColumnID>& groupby_column_ids) : AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

    const std::string AggregateSort::name() const  { return "AggregateSort"; }

    const std::string AggregateSort::description(DescriptionMode description_mode) const {
      return "TODO: insert description here";
    }

    void _write_result_to_result_table(const std::shared_ptr<Table> result_table, const std::vector<AllTypeVariant>& previous_values, const std::vector<AllTypeVariant>& current_aggregate_results){
        //write current aggregate value and reset current value
        std::vector<AllTypeVariant> new_values;
        new_values.reserve(previous_values.size() + current_aggregate_results.size());
        new_values.insert(new_values.end(), previous_values.begin(), previous_values.end());
        new_values.insert(new_values.end(), current_aggregate_results.begin(), current_aggregate_results.end());
        result_table->append(new_values);
    }

    std::shared_ptr<const Table> AggregateSort::_on_execute() {
        const auto input_table = input_table_left();
        //create table with correct schema
        for (const auto column_id : _groupby_column_ids) {
            _output_column_definitions.emplace_back(input_table->column_name(column_id), input_table->column_data_type(column_id));
        }
        for (const auto& aggregate : _aggregates) {
            //_output_column_definitions.emplace_back(input_table->column_name(_aggregates.column) + TODO AGGREGATENAME, input_table->column_data_type(_aggregates.column));
            _output_column_definitions.emplace_back("MIN(" + input_table->column_name(aggregate.column.value_or(ColumnID{42})) + ")", DataType::Float);
        }
        auto result_table = std::make_shared<Table>(_output_column_definitions, TableType::Data);

        if(input_table->empty()) {
            return result_table;
        }

        //sort by group by columns
        //TODO for now only first group by column
        //for (ColumnID column_id : _groupby_column_ids) {
        auto sorted_table = input_table;
        if (_groupby_column_ids.size() > 0) {
            Sort sort = Sort(_input_left, _groupby_column_ids[0]);
            sort.execute();
            sorted_table = sort.get_output();
        }
        //}

        //*
        // initialize previous values with values in first row
        std::vector<AllTypeVariant> previous_values;
        previous_values.reserve(_groupby_column_ids.size());

        for (const auto column_id : _groupby_column_ids) {
            previous_values.emplace_back(sorted_table->get_value<int>(column_id, size_t(0u)));
        }

        std::vector<AllTypeVariant> current_aggregate_results(_aggregates.size());
        current_aggregate_results[0] = 2000000.f;
        //*
        auto chunks = sorted_table->chunks();
        for (const auto& chunk : chunks) {
            size_t chunk_size = chunk->size();
            const auto segments = chunk->segments();
            for (ChunkOffset offset{0};offset < chunk_size;offset++) {
                std::vector<AllTypeVariant> current_values;
                current_values.reserve(_groupby_column_ids.size());


                for (size_t index = 0; index < _groupby_column_ids.size(); index++) {
                    AllTypeVariant current_value = segments[_groupby_column_ids[index]]->operator[](offset);
                    current_values.emplace_back(current_value);
                }


                if (current_values == previous_values) {

                    for (size_t index = 0; index < current_aggregate_results.size(); index++) {
                        current_aggregate_results[index] = min(current_aggregate_results[index],
                                                           segments[*_aggregates[index].column]->operator[](offset));
                    }
                } else {
                    _write_result_to_result_table(result_table, previous_values, current_aggregate_results);

                    previous_values = current_values;
                    for (size_t index = 0; index < current_aggregate_results.size(); index++) {
                        current_aggregate_results[index] = segments[*_aggregates[index].column]->operator[](offset);
                    }
                }

            }
        }
        _write_result_to_result_table(result_table, previous_values, current_aggregate_results);

         //*/
        return result_table;
    }

    std::shared_ptr<AbstractOperator> AggregateSort::_on_deep_copy(
            const std::shared_ptr<AbstractOperator>& copied_input_left,
            const std::shared_ptr<AbstractOperator>& copied_input_right) const {
        return std::make_shared<AggregateSort>(copied_input_left, _aggregates, _groupby_column_ids);
    }

    void AggregateSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

    void AggregateSort::_on_cleanup() {}
} // namespace opossum