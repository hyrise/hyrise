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

    std::shared_ptr<const Table> AggregateSort::_on_execute() {
        //sort by group by columns
        //TODO for now only first group by column
        //for (ColumnID column_id : _groupby_column_ids) {
        auto sorted_table = input_table_left();
        if (_groupby_column_ids.size() > 0) {
            Sort sort = Sort(_input_left, _groupby_column_ids[0]);
            sort.execute();
            sorted_table = sort.get_output();
        }
        //}
/*
        std::vector<AllTypeVariant> previous_values (_groupby_column_ids.size());
        //std::vector<AllTypeVariant> current_aggregate_results<AllTypeVariants>(_aggregates.size());
        AllTypeVariant current_aggregate_value;
        Table result_table;

        auto chunks = sorted_table->chunks();
        for (const auto& chunk : chunks) {
            size_t chunk_size = chunk->size();
            const auto segments = chunk->segments();
            for (ChunkOffset offset{0};offset < chunk_size;offset++) {
                bool changed = false;
                for (size_t index = 0;index < _groupby_column_ids.size();index++) {
                    AllTypeVariant current_value = segments[_groupby_column_ids[index]]->operator[](offset);
                    if (previous_values[index] != current_value) {
                        changed = true;
                        previous_values[index] = current_value;
                    }
                }

                if (changed) {
                    //write current aggregate value and reset current value
                    std::vector<AllTypeVariant> new_values(_groupby_column_ids.size() + _aggregates.size());


                    //result_table
                }

                if (_aggregates.size() > 0) {
                    //TODO: write last group by value
                    current_aggregate_value = min(current_aggregate_value, segments[_aggregates[0]]->operator[](offset));
                }

            }
        }
*/
        return sorted_table;
    }

    std::shared_ptr<AbstractOperator> AggregateSort::_on_deep_copy(
            const std::shared_ptr<AbstractOperator>& copied_input_left,
            const std::shared_ptr<AbstractOperator>& copied_input_right) const {
        return std::make_shared<AggregateSort>(copied_input_left, _aggregates, _groupby_column_ids);
    }

    void AggregateSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

    void AggregateSort::_on_cleanup() {}
} // namespace opossum