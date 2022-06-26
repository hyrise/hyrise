#include "lqp_parse_plugin.hpp"

#include "magic_enum.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "storage/table.hpp"

namespace opossum {

    std::string LQPParsePlugin::description() const { return "Hyrise LQPParsePlugin"; }

    UCCCandidate* LQPParsePlugin::generate_valid_candidate(std::shared_ptr<AbstractLQPNode> root_node, std::shared_ptr<LQPColumnExpression> column_candidate) {
        UCCCandidate* candidate = nullptr;

        if (!root_node) {
            // input node may already be nullptr in case we try to get right input of node with only one input
            return nullptr;
        } else {
            visit_lqp(root_node, [&](auto& node) {
                if (node->type != LQPNodeType::Predicate) {
                    return LQPVisitation::VisitInputs;
                }

                std::cout << "\t\t\t\t" << node->description() << std::endl;
                
                // when looking at predicate node, check whether the searched column is filtered in this predicate
                // -> if so, it is a valid UCC candidate; if not, still continue search
                const auto casted_node = std::static_pointer_cast<PredicateNode>(node);

                // first, ensure that we look at a binary predicate expression checking for equality (e.g., A==B)
                const auto predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(casted_node->predicate());
                if (!predicate) {
                    return LQPVisitation::VisitInputs;
                }
                
                auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->left_operand());
                if (!column_expression) {
                    column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->right_operand());
                }

                if ((!column_expression) || (column_expression != column_candidate)) {
                    return LQPVisitation::VisitInputs;
                }

                const auto table = std::static_pointer_cast<const StoredTableNode>(column_expression->original_node.lock());
                candidate = new UCCCandidate{table->table_name, column_expression->original_column_id};

                return LQPVisitation::DoNotVisitInputs;
            });
        }

        return candidate;
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
                        const auto casted_candidate = std::dynamic_pointer_cast<LQPColumnExpression>(column_candidate);
                        if (!casted_candidate) {
                            continue;
                        }
                        const auto table = std::static_pointer_cast<const StoredTableNode>(casted_candidate->original_node.lock());
                        const auto candidate = new UCCCandidate{table->table_name, casted_candidate->original_column_id};
                        if (candidate) {
                            std::cout << "\t\t\tAdding candidate " << candidate->table_name() << candidate->column_id() << std::endl;
                            ucc_candidates.push_back(*candidate);
                        }
                    }
                    
                    return LQPVisitation::VisitInputs;
                }
                
                // if not aggregate node, must be a join node
                auto& join_node = static_cast<JoinNode&>(*node);
                const auto& join_mode = join_node.join_mode;
                auto join_predicate = std::static_pointer_cast<BinaryPredicateExpression>(join_node.join_predicates().at(0));

                // we only care about inner (both are potential candidates), right outer (left is potential candidate) and left outer (right is potential candidate) joins
                switch (join_mode) {
                    case JoinMode::Semi:
                        // in Hyrise, it seems as if left-hand side is no longer used -> check that for UCC; ignore right
                    case JoinMode::Right: {
                        // want to check only the left hand side here, as this is the one that will be removed in the end
                        auto column_candidate = std::static_pointer_cast<LQPColumnExpression>(join_predicate->left_operand());
                        auto subtree_root = join_node.left_input();
                        if (!expression_evaluable_on_lqp(column_candidate, *subtree_root.get())) {
                            subtree_root = join_node.right_input();
                        }
                        std::cout << "\t\tChecking for candidate " << column_candidate->as_column_name() << std::endl;
                        auto candidate = generate_valid_candidate(subtree_root, column_candidate);
                        if (candidate) {
                            std::cout << "\t\t\tAdding candidate " << candidate->table_name() << candidate->column_id() << std::endl;
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
                            auto subtree_root = join_node.left_input();
                            if (!expression_evaluable_on_lqp(column_candidate, *subtree_root.get())) {
                                subtree_root = join_node.right_input();
                            }
                            auto candidate = generate_valid_candidate(subtree_root, std::static_pointer_cast<LQPColumnExpression>(column_candidate));
                            if (candidate) {
                                std::cout << "\t\t\tAdding candidate " << candidate->table_name() << candidate->column_id() << std::endl;
                                ucc_candidates.push_back(*candidate);
                            }
                            left = false;
                        }
                        break;
                    }

                    case JoinMode::Left: {
                        // want to check only the right hand side here, as this is the one that will be removed in the end
                        auto column_candidate = std::static_pointer_cast<LQPColumnExpression>(join_predicate->right_operand());
                        std::cout << "\t\tChecking for candidate " << column_candidate->as_column_name() << std::endl;
                        auto subtree_root = join_node.left_input();
                        if (!expression_evaluable_on_lqp(column_candidate, *subtree_root.get())) {
                            subtree_root = join_node.right_input();
                        }
                        auto candidate = generate_valid_candidate(subtree_root, column_candidate);
                        if (candidate) {
                            std::cout << "\t\t\tAdding candidate " << candidate->table_name() << candidate->column_id() << std::endl;
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
