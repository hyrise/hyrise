#include "lqp_parse_plugin.hpp"

#include "magic_enum.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "expression/logical_expression.hpp"
#include "storage/table.hpp"

namespace opossum {

    std::string LQPParsePlugin::description() const { return "Hyrise LQPParsePlugin"; }

    UCCCandidate* LQPParsePlugin::generate_valid_candidate(std::shared_ptr<AbstractLQPNode> node, std::shared_ptr<AbstractExpression> column, bool validated = false) {
                UCCCandidate* left_sub_result = nullptr;
                UCCCandidate* right_sub_result = nullptr;
                
                if (!node) {
                    // input node may already be nullptr in case we try to get right input of node with only one input
                    return nullptr;
                } else if (node->type == LQPNodeType::Predicate) {
                    // when looking at predicate node, check whether the searched column is filtered in this predicate
                    // -> if so, it is a valid UCC candidate, so pass that info to further search; if not, still continue search
                    const auto casted_node = std::static_pointer_cast<PredicateNode>(node);
                    std::cout << "\t\t\t\t" << node->description() << std::endl;

                    const auto predicate = std::static_pointer_cast<LogicalExpression>(casted_node->predicate());
                    bool is_valid = predicate->left_operand()->description() == column->description();

                    // recurse through the left and right side of tree, only touch right if left isn't already returning valid UCC candidate
                    left_sub_result = generate_valid_candidate(node->left_input(), column, is_valid);
                    if (!left_sub_result) {
                        right_sub_result = generate_valid_candidate(node->right_input(), column, is_valid);
                    }
                } else if (node->type == LQPNodeType::StoredTable) {
                    // when looking at stored table node, check whether it contains the column searched for
                    // if so, return the table name and node ID as UCCCandidate object, otherwise return nullptr
                    // TODO: what happens if we run into two different tables having a colunm with the same name? Probably need to look at that
                    std::cout << "\t\t\t\t" << node->description() << std::endl;
                    if (!validated) {
                        // if column would not be a valid UCC candidate, skip verification
                        return nullptr;
                    } else {
                        const auto casted_node = std::static_pointer_cast<StoredTableNode>(node);
                        const auto& table_name = casted_node->table_name;
                        // verify by checking whether the node is able to fetch the column from its table, if not, return nullptr
                        try {
                            casted_node->get_column(column->as_column_name());
                        } catch (const std::exception& exception) {
                            return nullptr;
                        }
                        // get column ID from its Expression
                        const auto& column_id = casted_node->get_column_id(*column);
                        return new UCCCandidate(table_name, column_id);
                    }
                } else {
                    // if neither of the special cases handled, simply recurse further through the tree, depth first left to right
                    // right paths are skipped once a valid UCC is found to improve performance
                    left_sub_result = generate_valid_candidate(node->left_input(), column, validated);
                    if (!left_sub_result) {
                        right_sub_result = generate_valid_candidate(node->right_input(), column, validated);
                    }
                }

                return (left_sub_result) ? left_sub_result : right_sub_result;
            }

    void LQPParsePlugin::start() {
        const auto snapshot = Hyrise::get().default_lqp_cache->snapshot();

        auto ucc_candidates = UCCCandidates{};

        for (const auto& [query, entry] : snapshot) {
            std::cout << "\n" << query << std::endl;
            const auto& root_node = entry.value;

            // TODO: really use a vector, or deduplicate by using a set?

            visit_lqp(root_node, [&](auto& node) {
                const auto type = node->type;
                if ((type != LQPNodeType::Join) && (type != LQPNodeType::Aggregate)) {
                    // Non-Join and Non-Aggregate (Groupby) nodes are not considered for optimization using UCCs
                    return LQPVisitation::VisitInputs;
                }

                std::cout << "\t" << node->description() << std::endl;

                if (type == LQPNodeType::Aggregate) {
                    // in case of aggregate, extract all predicates used in groupby operations, then try to generate UCCCandidate objects from each of them
                    auto& aggregate_node = static_cast<AggregateNode&>(*node);
                    auto column_candidates = std::vector<std::shared_ptr<AbstractExpression>>{aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.begin() + aggregate_node.aggregate_expressions_begin_idx};
                    
                    for (const auto& column_candidate : column_candidates) {
                        std::cout << "\t\tChecking for candidate " << column_candidate->as_column_name() << std::endl;
                        auto candidate = generate_valid_candidate(aggregate_node.left_input(), column_candidate, true);
                         if (candidate) {
                            std::cout << "\t\t\tAdding candidate..." << std::endl;
                            ucc_candidates.push_back(*candidate);
                        }
                    }
                    
                    return LQPVisitation::VisitInputs;
                }
                
                // if not aggregate node, must be a join node
                auto& join_node = static_cast<JoinNode&>(*node);
                const auto& join_mode = join_node.join_mode;
                auto join_predicate = std::static_pointer_cast<LogicalExpression>(join_node.join_predicates().at(0));

                // we only care about inner (both are potential candidates), right outer (left is potential candidate) and left outer (right is potential candidate) joins
                switch (join_mode) {
                    case JoinMode::Right: {
                        // want to check only the left hand side here, as this is the one that will be removed in the end
                        auto column_candidate = join_predicate->left_operand();
                        auto candidate = generate_valid_candidate(join_node.left_input(), column_candidate, false);
                        std::cout << "\t\tChecking for candidate " << column_candidate->as_column_name() << std::endl;
                        if (candidate) {
                            std::cout << "\t\t\tAdding candidate..." << std::endl;
                            ucc_candidates.push_back(*candidate);
                        }
                        break;
                    }

                    case JoinMode::Inner: {
                        auto column_candidates = std::vector<std::shared_ptr<AbstractExpression>>{join_predicate->left_operand(), join_predicate->right_operand()};
                        bool left = true;
                        for (const auto& column_candidate : column_candidates) {
                            std::cout << "\t\tRunning through " << ((left) ? "left" : "right") << " on InnerJoin" << std::endl;
                            std::cout << "\t\tChecking for candidate " << column_candidate->as_column_name() << std::endl;
                            auto candidate = generate_valid_candidate((left) ? join_node.left_input() : join_node.right_input(), column_candidate, false);
                            if (candidate) {
                                std::cout << "\t\t\tAdding candidate..." << std::endl;
                                ucc_candidates.push_back(*candidate);
                            }
                            left = false;
                        }
                        break;
                    }

                    case JoinMode::Left: {
                        // want to check only the right hand side here, as this is the one that will be removed in the end
                        auto column_candidate = join_predicate->right_operand();
                        auto candidate = generate_valid_candidate(join_node.right_input(), column_candidate, false);
                        std::cout << "\t\tChecking for candidate " << column_candidate->as_column_name() << std::endl;
                        if (candidate) {
                            std::cout << "\t\t\tAdding candidate..." << std::endl;
                            ucc_candidates.push_back(*candidate);
                        }
                        break;
                    }

                    default: {}
                }

                // TO DO

                // Identify whether the join operation could be improved by using UCCs
                // .. Check Node type - we only want to utilize UCCs for join elimination and Aggregation simplifying
                // -- which join types can actually be optimized? Only Inner? In case of left/right inner/outer, the one being dropped is the interesting side
                // ... If we see join, check whether predicate filter is done on same table and different column prior to join
                // ... Column being filtered on and join column are candidates
                // INTERFACE: UCC Validation Candidate: std::vector of table name & column ID -> get table name by traversing to leaf, then it's instance variable of StoredTableNode
                // Validate UCCs
                // .. Iterate over UCCCandidates; For each candidate:
                // ... Get all of its segments
                // ... Start with Dictionary Segments -> if len(dictionary) == len(attributevector) then UCC constraint still intact
                // ... If all individual segments match constraint, do value validation -> merge dictionaries, if collision UCC broken
                // ... If still everything valid, look at non-dictionary segments, check whether all values stored are not yet in your merged value list
                // --- If we want to use the GroupBy (Aggregation operator) -> put all values in one big vector, run operator on it, check whether #bins == #values
                // ... Add information gained to table -> also remember which columns are not UCCs?

                return LQPVisitation::VisitInputs;
            });
        }

        std::cout << "------------------" << std::endl;
        std::cout << "UCC candidates: " << ucc_candidates.size() << std::endl;
    }

    void LQPParsePlugin::stop() {}

    EXPORT_PLUGIN(LQPParsePlugin)

}  // namespace opossum
