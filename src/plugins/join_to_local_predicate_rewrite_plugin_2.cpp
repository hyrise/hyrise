/*
 *  In this plugin, we implement one way for UCC dependency discovery in table columns.
 *  In general, sort and adjacent_find are used for duplicate detection. The sort is optimized for sorted sub-vectors which are merged as a whole.
 *  Early outs are exploited for dictionary segments.
 */

#include "join_to_local_predicate_rewrite_plugin.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "magic_enum.hpp"

#include "expression/binary_predicate_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"

#include "resolve_type.hpp"

namespace opossum {

std::string JoinToLocalPredicateRewritePlugin::description() const {
    return "This is the Hyrise JoinToLocalPredicateRewritePlugin";
}

void JoinToLocalPredicateRewritePlugin::start() {
    // _loop_thread_start = std::make_unique<PausableLoopThread>(JoinToLocalPredicateRewritePlugin::IDLE_DELAY_PREDICATE_REWRITE, [&](size_t) { _start(); });
    _start();
}

void check_and_add_unique_constraint(const std::shared_ptr<LQPColumnExpression> candidate) {
    const auto& table_node = std::dynamic_pointer_cast<const StoredTableNode>(candidate->original_node.lock());
    const auto& table = Hyrise::get().storage_manager.get_table(table_node->table_name);
    const auto& soft_key_constraints = table->soft_key_constraints();

    const auto col_id = candidate->original_column_id;
    const auto chunk_count = table->chunk_count();

    // Skip already discovered unique constraints.
    if (std::any_of(begin(soft_key_constraints), end(soft_key_constraints), [&col_id](const auto key_constraint) {
        const auto& columns = key_constraint.columns();
        return columns.size() == 1 && columns.contains(col_id);
    })) {
        return;
    }

    const auto num_rows = table->row_count();

    resolve_data_type(table->column_data_type(col_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        using VectorIterator = pmr_vector<ColumnDataType>::iterator;

        // We need to remember if the column contains compressed or uncompressed values.
        // For mixed compressed and uncompressed segments, we can't benefit from pre-sorted sub-vectors, so we treat these columns the same as uncompressed ones.
        auto compressed = true;

        // all_values contains the segment values from all chunks.
        auto all_values = std::unique_ptr<pmr_vector<ColumnDataType>>();
        // We remember the start iterators of the sub-vectors in all_values that can be merged for pure compressed columns. No random access is needed, so a list is used for performance reasons.
        auto start_iterators = std::list<VectorIterator>{};

        // We can use an early-out if we find a single dict segment that contains a duplicate.
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id ++) {
            const auto& source_chunk = table->get_chunk(chunk_id);
            const auto& source_segment = source_chunk->get_segment(col_id);

            if (std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment)) {
                const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment);
                const auto& dict = dict_segment->dictionary();
                const auto& attr_vector = dict_segment->attribute_vector();

                if (dict->size() != attr_vector->size()) {
                    return;
                }
            }
        }

        // If we reach here, we have to make a cross-segment duplicate check.
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id ++) {
            const auto& source_chunk = table->get_chunk(chunk_id);
            const auto& source_segment = source_chunk->get_segment(col_id);

            if (chunk_id == 0) {
                all_values = std::make_unique<pmr_vector<ColumnDataType>>();
                all_values->reserve(num_rows);
            }

            if (std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(source_segment)) {
                const auto& val_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(source_segment);
                const auto& values = val_segment->values();

                compressed = false;

                std::copy(begin(values), end(values), std::back_inserter(*all_values));

                std::cout << values.size() << std::endl;
                std::cout << "----" << std::endl;
            } else if (std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment)) {
                const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment);
                const auto& dict = dict_segment->dictionary();
                const auto& attr_vector = dict_segment->attribute_vector();
                
                start_iterators.push_back(all_values->end());
                std::copy(begin(*dict), end(*dict), std::back_inserter(*all_values));

                std::cout << dict->size() << std::endl;
                std::cout << attr_vector->size() << std::endl;
                std::cout << "----" << std::endl;
            } else {
                Fail("The given segment type is not supported for the discovery of UCCs.");
            }
        }

        if (compressed) {
            // We merge the sorted sub-vectors until there is only one sub-vector left. Then, all_values is sorted.
            while (start_iterators.size() > 1) {
                for (auto start_it = begin(start_iterators); start_it != start_iterators.end() && std::next(start_it, 1) != start_iterators.end(); start_it ++) {
                    std::inplace_merge(*start_it, *(std::next(start_it, 1)), *(std::next(start_it, 2)));
                    start_iterators.erase(std::next(start_it, 1));
                }
            }
        } else {
            // There is no guarantee for sorted sub-vectors of a certain length, so we conventionally sort.
            std::sort(begin(*all_values), end(*all_values));
        }

        if (std::unique(begin(*all_values), end(*all_values)) == all_values->end()) {
            // We save UCC constraints directly inside the table so they can be forwarded to nodes in a query plan.
            std::cout << "Discovered UCC candidate: " << table->column_name(col_id) << std::endl;
            table->add_soft_key_constraint(TableKeyConstraint(std::unordered_set(std::initializer_list<ColumnID>{col_id}), KeyConstraintType::UNIQUE));
        }
        std::cout << std::endl;
    });
}

void JoinToLocalPredicateRewritePlugin::_start() {
    std::cout << "The Hyrise JoinToLocalPredicateRewritePlugin was started..." << std::endl;

    const auto& snapshot = Hyrise::get().default_lqp_cache->snapshot();

    for (const auto& [query, entry] : snapshot) {
        std::cout << query << std::endl;
        const auto& root_node = entry.value;
        
        visit_lqp(root_node, [](auto& node) {
            const auto type = node->type;
            std::cout << "\t" << magic_enum::enum_name(type) << std::endl;

            // Print some node information for orientation.
            if (node->type == LQPNodeType::Join || node->type  == LQPNodeType::StoredTable) {
                std::cout << "\t\t" << node->description() << std::endl;

                // Iterate over unique constraints of join.
                const auto& unique_constraints = node->unique_constraints();

                std::cout << "\t\tUCCs: ";
                for (const auto& unique_constraint: *unique_constraints) {
                    std::cout << unique_constraint;
                }
                std::cout << std::endl;
            }

            if (node->type == LQPNodeType::Join) {
                const auto& join_node = std::dynamic_pointer_cast<JoinNode>(node);
                const auto& join_predicates = join_node->join_predicates();

                auto join_cols = std::vector<std::shared_ptr<LQPColumnExpression>>{};
                for (const auto& join_predicate: join_predicates) {
                    const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);

                    if (std::dynamic_pointer_cast<LQPColumnExpression>(binary_predicate->left_operand())) {
                        join_cols.push_back(std::dynamic_pointer_cast<LQPColumnExpression>(binary_predicate->left_operand()));
                    }
                    if (std::dynamic_pointer_cast<LQPColumnExpression>(binary_predicate->right_operand())) {
                        join_cols.push_back(std::dynamic_pointer_cast<LQPColumnExpression>(binary_predicate->right_operand()));
                    }
                }

                for (const auto& join_col: join_cols) {
                    check_and_add_unique_constraint(join_col);
                }
            } else if (node->type == LQPNodeType::Predicate) {
                const auto& pred_node = std::dynamic_pointer_cast<PredicateNode>(node);
                const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(pred_node->predicate());
                
                auto pred_col = std::shared_ptr<LQPColumnExpression>{};
                if (std::dynamic_pointer_cast<LQPColumnExpression>(predicate->left_operand()) && std::dynamic_pointer_cast<ValueExpression>(predicate->right_operand())) {
                    pred_col = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->left_operand());
                } else if (std::dynamic_pointer_cast<LQPColumnExpression>(predicate->right_operand()) && std::dynamic_pointer_cast<ValueExpression>(predicate->left_operand())) {
                    pred_col = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->right_operand());
                }

                check_and_add_unique_constraint(pred_col);
            }

            return LQPVisitation::VisitInputs;
        });
    }
}

void JoinToLocalPredicateRewritePlugin::stop() {
    std::cout << "The Hyrise JoinToLocalPredicateRewritePlugin was stopped..." << std::endl;

    // _loop_thread_start.reset();
}

EXPORT_PLUGIN(JoinToLocalPredicateRewritePlugin)

}
