#pragma once

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Aggregates are defined by the column (ColumnID for Operators, LQPColumnReference in LQP) they operate on and the aggregate
 * function they use. COUNT() is the exception that doesn't use a column, which is why column is optional
 * Optionally, an alias can be specified to use as the output name.
 */
    struct AggregateColumnDefinition final {
        AggregateColumnDefinition(const std::optional <ColumnID> &column, const AggregateFunction function)
                : column(column), function(function) {}

        std::optional <ColumnID> column;
        AggregateFunction function;
    };

    class AbstractAggregateOperator : public AbstractReadOnlyOperator {
    public:
        AbstractAggregateOperator(const std::shared_ptr <AbstractOperator> &in,
                                  const std::vector <AggregateColumnDefinition> &aggregates,
                                  const std::vector <ColumnID> &groupby_column_ids);

        const std::vector <AggregateColumnDefinition> &aggregates() const;

        const std::vector <ColumnID> &groupby_column_ids() const;
        
        virtual const std::string name() const override = 0;

        virtual const std::string description(DescriptionMode description_mode) const override = 0;

    protected:
        virtual std::shared_ptr<const Table> _on_execute() = 0;

        const std::vector <AggregateColumnDefinition> _aggregates;
        const std::vector <ColumnID> _groupby_column_ids;
    };

} // namespace opossum