#include "aggregate_sort.hpp"

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "type_cast.hpp"
#include "operators/sort.hpp"
#include "aggregate/aggregate_traits.hpp"

namespace opossum {

    AggregateSort::AggregateSort(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                  const std::vector<ColumnID>& groupby_column_ids) : AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

    const std::string AggregateSort::name() const  { return "AggregateSort"; }

    const std::string AggregateSort::description(DescriptionMode description_mode) const {
      return "TODO: insert description here";
    }

    template<typename ColumnType, typename AggregateType>
    void AggregateSort::_aggregate_values(std::vector<AllTypeVariant>& previous_values, std::vector<std::vector<AllTypeVariant>>& groupby_keys, std::vector<std::vector<AllTypeVariant>>& aggregate_results, uint64_t aggregate_index, AggregateFunctor<ColumnType, AggregateType> aggregate_function, std::shared_ptr<const Table> sorted_table) {
        std::optional<AggregateType> current_aggregate_value;

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
                    const ColumnType new_value = type_cast_variant<ColumnType>(segments[*_aggregates[aggregate_index].column]->operator[](offset));
                    //const AggregateType old_value = type_cast_variant<AggregateType>(current_aggregate_value);
                    aggregate_function(new_value, current_aggregate_value);
                } else {
                    aggregate_results[aggregate_index].emplace_back(*current_aggregate_value);
                    if (aggregate_index == 0) {
                        for (size_t groupby_id = 0;groupby_id < _groupby_column_ids.size();groupby_id++) {
                            groupby_keys[groupby_id].emplace_back(previous_values[groupby_id]);
                        }
                    }

                    previous_values = current_values;
                    current_aggregate_value = type_cast_variant<AggregateType>(segments[*_aggregates[aggregate_index].column]->operator[](offset));
                }

            }
        }
        aggregate_results[aggregate_index].emplace_back(*current_aggregate_value);
        if (aggregate_index == 0) {
            for (size_t groupby_id = 0;groupby_id < _groupby_column_ids.size();groupby_id++) {
                groupby_keys[groupby_id].emplace_back(previous_values[groupby_id]);
            }
        }
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

        std::vector<std::vector<AllTypeVariant>> aggregate_results(_aggregates.size());
        std::vector<std::vector<AllTypeVariant>> groupby_keys(_groupby_column_ids.size());

        uint64_t aggregate_index = 0;
        for (const auto& aggregate : _aggregates) {
            auto data_type = input_table->column_data_type(*aggregate.column);
            resolve_data_type(data_type, [&, aggregate](auto type) {
                using ColumnDataType = typename decltype(type)::type;

                switch (aggregate.function) {
                    case AggregateFunction::Min: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Min>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Min>().get_aggregate_function();
                        _aggregate_values(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::Max: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Max>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Max>().get_aggregate_function();
                        _aggregate_values(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::Sum: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Sum>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum>().get_aggregate_function();
                        _aggregate_values(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }

                    case AggregateFunction::Avg: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Avg>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Avg>().get_aggregate_function();
                        _aggregate_values(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::Count: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Count>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Count>().get_aggregate_function();
                        _aggregate_values(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::CountDistinct: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::CountDistinct>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct>().get_aggregate_function();
                        _aggregate_values(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                }


            });

            aggregate_index++;
        }

        const size_t num_groups = !groupby_keys.empty() ? groupby_keys[0].size() : aggregate_results[0].size();

        std::vector<AllTypeVariant> result_line(_groupby_column_ids.size() + _aggregates.size());
        for (size_t group_index = 0;group_index < num_groups;group_index++) {
            for (size_t groupby_index = 0;groupby_index < _groupby_column_ids.size();groupby_index++) {
                result_line[groupby_index] = groupby_keys[groupby_index][group_index];
            }
            for (size_t aggregate_index = 0;aggregate_index < _aggregates.size();aggregate_index++) {
                result_line[_groupby_column_ids.size() + aggregate_index] = aggregate_results[aggregate_index][group_index];
            }
            result_table->append(result_line);
        }
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