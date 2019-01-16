#include "aggregate_sort.hpp"

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "table_wrapper.hpp"
#include "types.hpp"
#include "type_cast.hpp"
#include "operators/sort.hpp"
#include "aggregate/aggregate_traits.hpp"
#include "constant_mappings.hpp"

namespace opossum {

    AggregateSort::AggregateSort(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                  const std::vector<ColumnID>& groupby_column_ids) : AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

    const std::string AggregateSort::name() const  { return "AggregateSort"; }

    const std::string AggregateSort::description(DescriptionMode description_mode) const {
      return "TODO: insert description here";
    }

    bool compare_all_type_variant(const AllTypeVariant& a, const AllTypeVariant& b) {
        if (variant_is_null(a) && variant_is_null((b))) {
            return true;
        } else if (variant_is_null(a) != variant_is_null(b)) {
            return false;
        } else {
            return a == b;
        }
    }

    template<typename ColumnType, typename AggregateType, AggregateFunction function>
    void AggregateSort::_aggregate_values(std::vector<AllTypeVariant>& previous_values, std::vector<std::vector<AllTypeVariant>>& groupby_keys, std::vector<std::vector<AllTypeVariant>>& aggregate_results, uint64_t aggregate_index, AggregateFunctor<ColumnType, AggregateType> aggregate_function, std::shared_ptr<const Table> sorted_table) {
        // initialize previous values with values in first row
        previous_values.reserve(_groupby_column_ids.size());

        for (const auto column_id : _groupby_column_ids) {
            previous_values.emplace_back(sorted_table->chunks()[0]->segments()[column_id]->operator[](0));
        }

        std::optional<AggregateType> current_aggregate_value;
        uint64_t value_count = 0u;
        uint64_t value_count_with_null = 0u;

        std::unordered_set<ColumnType> unique_values;

        auto chunks = sorted_table->chunks();
        for (const auto& chunk : chunks) {
            size_t chunk_size = chunk->size();
            const auto segments = chunk->segments();
            for (ChunkOffset offset{0}; offset < chunk_size; offset++) {
                std::vector<AllTypeVariant> current_values;
                current_values.reserve(_groupby_column_ids.size());

                for (const auto column_id : _groupby_column_ids) {
                    AllTypeVariant current_value = segments[column_id]->operator[](offset);
                    current_values.emplace_back(current_value);
                }

                const auto groupby_key_unchanged = std::equal(current_values.cbegin(), current_values.cend(), previous_values.cbegin(), compare_all_type_variant);

                AllTypeVariant new_value_raw;
                if (function == AggregateFunction::Count && !_aggregates[aggregate_index].column) {
                    new_value_raw = NULL_VALUE;
                } else {
                    new_value_raw = segments[*_aggregates[aggregate_index].column]->operator[](offset);
                }
                if (groupby_key_unchanged) {
                    if (!variant_is_null(new_value_raw)) {
                        const auto new_value = type_cast_variant<ColumnType>(new_value_raw);
                        aggregate_function(new_value, current_aggregate_value);
                        value_count++;
                        if constexpr (function == AggregateFunction::CountDistinct) {
                            unique_values.insert(new_value);
                        }
                    }
                    value_count_with_null++;
                } else {
                    _set_and_write_aggregate_value<ColumnType, AggregateType, function>(previous_values, groupby_keys, aggregate_results, aggregate_index, current_aggregate_value, value_count, value_count_with_null, unique_values);
                    previous_values = current_values;
                    current_aggregate_value = std::optional<AggregateType>();
                    unique_values.clear();
                    value_count = 0u;
                    value_count_with_null = 1u;
                    if (!variant_is_null(new_value_raw)) {
                        const auto new_value = type_cast_variant<ColumnType>(new_value_raw);
                        aggregate_function(new_value, current_aggregate_value);
                        value_count = 1u;
                        if constexpr (function == AggregateFunction::CountDistinct) {
                            unique_values = {new_value};
                        }
                    }
                }

            }
        }
        _set_and_write_aggregate_value<ColumnType, AggregateType, function>(previous_values, groupby_keys, aggregate_results, aggregate_index, current_aggregate_value, value_count, value_count_with_null, unique_values);
    }

    template<typename ColumnType, typename AggregateType, AggregateFunction function>
    void AggregateSort::_set_and_write_aggregate_value(const std::vector<AllTypeVariant> &previous_values,
                                                      std::vector<std::vector<AllTypeVariant>> &groupby_keys,
                                                      std::vector<std::vector<AllTypeVariant>> &aggregate_results,
                                                      uint64_t aggregate_index,
                                                      std::optional<AggregateType> &current_aggregate_value,
                                                      uint64_t value_count  __attribute__((unused)),
                                                      uint64_t value_count_with_null  __attribute__((unused)),
                                                      const std::unordered_set<ColumnType> &unique_values) const {
        if constexpr (function == AggregateFunction::Count) {
            if (this->_aggregates[aggregate_index].column) {
                current_aggregate_value = value_count;
            } else {
                current_aggregate_value = value_count_with_null;
            }
        }
        if constexpr (function == AggregateFunction::Avg && std::is_arithmetic_v<AggregateType>) { //TODO arithmetic seems hacky
            if (value_count == 0) {
                current_aggregate_value = std::optional<AggregateType>();
            } else {
                current_aggregate_value = *current_aggregate_value / value_count;
            }

        }
        if constexpr (function == AggregateFunction::CountDistinct) {
            current_aggregate_value = unique_values.size();
        }
        if (current_aggregate_value.has_value()) {
            aggregate_results[aggregate_index].emplace_back(*current_aggregate_value);
        } else {
            aggregate_results[aggregate_index].emplace_back(NULL_VALUE);
        }
        if (aggregate_index == 0) {
            for (size_t groupby_id = 0;groupby_id < this->_groupby_column_ids.size(); groupby_id++) {
                groupby_keys[groupby_id].emplace_back(previous_values[groupby_id]);
            }
        }
    }



    std::shared_ptr<const Table> AggregateSort::_on_execute() {
        const auto input_table = input_table_left();

        // check for invalid aggregates
        for (const auto& aggregate : _aggregates) {
            if (!aggregate.column) {
                if (aggregate.function != AggregateFunction::Count) {
                    Fail("Aggregate: Asterisk is only valid with COUNT");
                }
            } else {
                DebugAssert(*aggregate.column < input_table->column_count(), "Aggregate column index out of bounds");
                if (input_table->column_data_type(*aggregate.column) == DataType::String &&
                    (aggregate.function == AggregateFunction::Sum || aggregate.function == AggregateFunction::Avg)) {
                    Fail("Aggregate: Cannot calculate SUM or AVG on string column");
                }
            }
        }

        //create table with correct schema
        for (const auto column_id : _groupby_column_ids) {
            _output_column_definitions.emplace_back(input_table->column_name(column_id), input_table->column_data_type(column_id), true);
        }

        ColumnID column_index{0};
        for (const auto& aggregate : _aggregates) {
            const auto column = aggregate.column;

            // Output column for COUNT(*). int is chosen arbitrarily.
            const auto data_type = !column ? DataType::Int : input_table->column_data_type(*column);

            resolve_data_type(data_type, [&, column_index](auto type) {
                _write_aggregate_output(type, column_index, aggregate.function);
            });

            ++column_index;
        }

        auto result_table = std::make_shared<Table>(_output_column_definitions, TableType::Data);

        if(input_table->empty()) {

            if (_groupby_column_ids.empty()) {
                std::vector<AllTypeVariant> default_values;
                for (const auto aggregate : _aggregates) {
                    if (aggregate.function == AggregateFunction::Count || aggregate.function == AggregateFunction::CountDistinct) {
                        default_values.emplace_back(AllTypeVariant{0});
                    } else {
                        default_values.emplace_back(NULL_VALUE);
                    }
                }
                result_table->append(default_values);
            }

            return result_table;
        }

        auto sorted_table = input_table;
        for(const auto column_id : _groupby_column_ids) {
            const auto sorted_wrapper = std::make_shared<TableWrapper>(sorted_table);
            sorted_wrapper->execute();
            Sort sort = Sort(sorted_wrapper, column_id);
            sort.execute();
            sorted_table = sort.get_output();
        }

        std::vector<std::vector<AllTypeVariant>> aggregate_results(_aggregates.size());
        std::vector<std::vector<AllTypeVariant>> groupby_keys(_groupby_column_ids.size());

        if(_aggregates.empty()){
            std::vector<AllTypeVariant> previous_values;
            previous_values.reserve(_groupby_column_ids.size());

            for (const auto column_id : _groupby_column_ids) {
                previous_values.emplace_back(sorted_table->chunks()[0]->segments()[column_id]->operator[](0));
            }

            auto chunks = sorted_table->chunks();
            for (const auto& chunk : chunks) {
                size_t chunk_size = chunk->size();
                const auto segments = chunk->segments();
                for (ChunkOffset offset{0}; offset < chunk_size; offset++) {
                    std::vector<AllTypeVariant> current_values;
                    current_values.reserve(_groupby_column_ids.size());

                    for (const auto column_id : _groupby_column_ids) {
                        AllTypeVariant current_value = segments[column_id]->operator[](offset);
                        current_values.emplace_back(current_value);
                    }
                    const auto groupby_key_changed = !std::equal(current_values.cbegin(), current_values.cend(), previous_values.cbegin(), compare_all_type_variant);
                    if (groupby_key_changed) {
                        for (size_t groupby_id = 0; groupby_id < _groupby_column_ids.size(); groupby_id++) {
                                groupby_keys[groupby_id].emplace_back(previous_values[groupby_id]);
                        }
                        previous_values = current_values;
                    }
                }
            }
            for (size_t groupby_id = 0; groupby_id < _groupby_column_ids.size(); groupby_id++) {
                groupby_keys[groupby_id].emplace_back(previous_values[groupby_id]);
            }
        }

        uint64_t aggregate_index = 0;
        for (const auto& aggregate : _aggregates) {

            std::vector<AllTypeVariant> previous_values;


            const auto data_type = !aggregate.column ? DataType::Int : input_table->column_data_type(*aggregate.column);
            resolve_data_type(data_type, [&, aggregate](auto type) {
                using ColumnDataType = typename decltype(type)::type;

                switch (aggregate.function) {
                    case AggregateFunction::Min: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Min>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Min>().get_aggregate_function();
                        _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Min>(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::Max: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Max>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Max>().get_aggregate_function();
                        _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Max>(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::Sum: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Sum>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum>().get_aggregate_function();
                        _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Sum>(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }

                    case AggregateFunction::Avg: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Avg>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Avg>().get_aggregate_function();
                        _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Avg>(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::Count: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::Count>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Count>().get_aggregate_function();
                        _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::Count>(previous_values, groupby_keys, aggregate_results, aggregate_index,
                                          aggregate_function, sorted_table);
                        break;
                    }
                    case AggregateFunction::CountDistinct: {
                        using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction::CountDistinct>::AggregateType;
                        auto aggregate_function = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct>().get_aggregate_function();
                        _aggregate_values<ColumnDataType, AggregateType, AggregateFunction::CountDistinct>(previous_values, groupby_keys, aggregate_results, aggregate_index,
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

    template <typename ColumnType>
    void AggregateSort::_write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                                            AggregateFunction function) {
        switch (function) {
            case AggregateFunction::Min:
                write_aggregate_output<ColumnType, AggregateFunction::Min>(column_index);
                break;
            case AggregateFunction::Max:
                write_aggregate_output<ColumnType, AggregateFunction::Max>(column_index);
                break;
            case AggregateFunction::Sum:
                write_aggregate_output<ColumnType, AggregateFunction::Sum>(column_index);
                break;
            case AggregateFunction::Avg:
                write_aggregate_output<ColumnType, AggregateFunction::Avg>(column_index);
                break;
            case AggregateFunction::Count:
                write_aggregate_output<ColumnType, AggregateFunction::Count>(column_index);
                break;
            case AggregateFunction::CountDistinct:
                write_aggregate_output<ColumnType, AggregateFunction::CountDistinct>(column_index);
                break;
        }
    }

    template <typename ColumnType, AggregateFunction function>
    void AggregateSort::write_aggregate_output(ColumnID column_index) {
        // retrieve type information from the aggregation traits
        auto aggregate_data_type = AggregateTraits<ColumnType, function>::AGGREGATE_DATA_TYPE;

        const auto& aggregate = _aggregates[column_index];

        if (aggregate_data_type == DataType::Null) {
            // if not specified, it’s the input column’s type
            aggregate_data_type = input_table_left()->column_data_type(*aggregate.column);
        }

        // Generate column name, TODO(anybody), actually, the AggregateExpression can do this, but the Aggregate operator
        // doesn't use Expressions, yet
        std::stringstream column_name_stream;
        if (aggregate.function == AggregateFunction::CountDistinct) {
            column_name_stream << "COUNT(DISTINCT ";
        } else {
            column_name_stream << aggregate_function_to_string.left.at(aggregate.function) << "(";
        }

        if (aggregate.column) {
            column_name_stream << input_table_left()->column_name(*aggregate.column);
        } else {
            column_name_stream << "*";
        }
        column_name_stream << ")";

        constexpr bool NEEDS_NULL = (function != AggregateFunction::Count && function != AggregateFunction::CountDistinct);
        _output_column_definitions.emplace_back(column_name_stream.str(), aggregate_data_type, NEEDS_NULL);
    }
} // namespace opossum