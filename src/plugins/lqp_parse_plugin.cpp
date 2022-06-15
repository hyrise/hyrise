#include "lqp_parse_plugin.hpp"

#include "magic_enum.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "storage/table.hpp"

namespace opossum {

    class UCCCandidate {
    protected:
        std::string _table_name;
        ColumnID _column_id;
    };

    using UCCCandidates = std::vector<UCCCandidate>;

    std::string LQPParsePlugin::description() const { return "Hyrise LQPParsePlugin"; }

    void LQPParsePlugin::start() {
        const auto snapshot = Hyrise::get().default_lqp_cache->snapshot();

        for (const auto& [query, entry] : snapshot) {
            std::cout << "\n" << query << std::endl;
            const auto& root_node = entry.value;

            visit_lqp(root_node, [](auto& node) {
                const auto type = node->type;
                std::cout << "\t" << magic_enum::enum_name(type) << std::endl;
                if (node->type != LQPNodeType::Join) {
                    return LQPVisitation::VisitInputs;
                }

                auto& join_node = static_cast<JoinNode&>(*node);
                std::cout << "\t\t" << join_node.description() << std::endl;

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
    }

    void LQPParsePlugin::stop() {}

    EXPORT_PLUGIN(LQPParsePlugin)

}  // namespace opossum
