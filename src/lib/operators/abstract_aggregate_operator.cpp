#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "abstract_aggregate_operator.hpp"

#include "types.hpp"

namespace opossum {

    AbstractAggregateOperator::AbstractAggregateOperator(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                                 const std::vector<ColumnID>& groupby_column_ids) : AbstractReadOnlyOperator(OperatorType::Aggregate, in), _aggregates{aggregates}, _groupby_column_ids{groupby_column_ids} {
        Assert(!(aggregates.empty() && groupby_column_ids.empty()),
               "Neither aggregate nor groupby columns have been specified");
    }

    const std::vector<AggregateColumnDefinition>& AbstractAggregateOperator::aggregates() const { return _aggregates; }
    const std::vector<ColumnID>& AbstractAggregateOperator::groupby_column_ids() const { return _groupby_column_ids; }

} // namespace opossum