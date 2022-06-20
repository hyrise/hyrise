/*
 *  In this plugin, we implement another way for UCC dependency discovery in table columns.
 *  Here, we use an unordered_set and range inserts to detect duplicates.
 *  For range inserts of n elements, an unordered_set has an average case complexity of O(n).
 *  On the other hand, ordered_sets have an average case complexity of O(n * log(size() + n)).
 *  Early outs are exploited for dictionary segments.
 */

#include "join_to_local_predicate_rewrite_plugin.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "magic_enum.hpp"

#include "resolve_type.hpp"

namespace opossum {

std::string JoinToLocalPredicateRewritePlugin::description() const {
    return "This is the Hyrise JoinToLocalPredicateRewritePlugin";
}

void JoinToLocalPredicateRewritePlugin::start() {
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

            if (node->type == LQPNodeType::StoredTable) {
                const auto& table_node = std::dynamic_pointer_cast<StoredTableNode>(node);

                const auto& table = Hyrise::get().storage_manager.get_table(table_node->table_name);
                std::cout << "\nColumn Definitions:" << std::endl;
                for (const auto& column_definition: table->column_definitions()) {
                    std::cout << column_definition << std::endl;
                }
                std::cout << std::endl;

                std::cout << "Key Constraints:" << std::endl;
                const auto& soft_key_constraints = table->soft_key_constraints();
                for (const auto& key_constraint: soft_key_constraints) {
                    for (const auto column: key_constraint.columns()) {
                        std::cout << table->column_name(column) << " ";
                    }
                    std::cout << std::endl;
                }
                std::cout << std::endl;

                const auto col_count = table->column_count();
                const auto chunk_count = table->chunk_count();

                for (auto col_id = ColumnID{0}; col_id < col_count; col_id ++) {
                    // Skip already discovered unique constraints.
                    if (std::any_of(begin(soft_key_constraints), end(soft_key_constraints), [&col_id](const auto key_constraint) {
                        const auto& columns = key_constraint.columns();
                        return columns.size() == 1 && columns.contains(col_id);
                    })) {
                        continue;
                    }

                    resolve_data_type(table->column_data_type(col_id), [&](const auto data_type_t) {
                        using ColumnDataType = typename decltype(data_type_t)::type;

                        // all_values collects the segment values from all chunks.
                        auto all_values = std::make_unique<std::unordered_set<ColumnDataType>>();
                        auto all_values_size = all_values->size();

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

                            if (std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(source_segment)) {
                                const auto& val_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(source_segment);
                                const auto& values = val_segment->values();

                                all_values->insert(begin(values), end(values));
                                if (all_values->size() == all_values_size + values.size()) {
                                    all_values_size += values.size();
                                } else {
                                    // If not all elements have been inserted, there must have occured a duplicate, so the UCC constraint is violated.
                                    return;
                                }

                                std::cout << values.size() << std::endl;
                                std::cout << "----" << std::endl;
                            } else if (std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment)) {
                                const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment);
                                const auto& dict = dict_segment->dictionary();
                                const auto& attr_vector = dict_segment->attribute_vector();
                                
                                all_values->insert(begin(*dict), end(*dict));
                                if (all_values->size() == all_values_size + dict->size()) {
                                    all_values_size += dict->size();
                                } else {
                                    // If not all elements have been inserted, there must have occured a duplicate, so the UCC constraint is violated.
                                    return;
                                }

                                std::cout << dict->size() << std::endl;
                                std::cout << attr_vector->size() << std::endl;
                                std::cout << "----" << std::endl;
                            } else {
                                Fail("The given segment type is not supported for the discovery of UCCs.");
                            }
                        }

                        // We save UCC constraints directly inside the table so they can be forwarded to nodes in a query plan.
                        std::cout << "Discovered UCC candidate: " << table->column_name(col_id) << std::endl;
                        table->add_soft_key_constraint(TableKeyConstraint(std::unordered_set(std::initializer_list<ColumnID>{col_id}), KeyConstraintType::UNIQUE));

                        std::cout << std::endl;
                    });
                }
            }

            return LQPVisitation::VisitInputs;
        });
    }
}

void JoinToLocalPredicateRewritePlugin::stop() {
    std::cout << "The Hyrise JoinToLocalPredicateRewritePlugin was stopped..." << std::endl;
}

EXPORT_PLUGIN(JoinToLocalPredicateRewritePlugin)

}
