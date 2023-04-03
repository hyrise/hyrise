
#include <set>

#include <boost/functional/hash.hpp>

#include "types.hpp"


namespace hyrise {

class StoredTableNode;
class TableStatistics;
struct OperatorScanPredicate;
class PredicateNode;

using PredicatePruningChain = std::vector<std::shared_ptr<PredicateNode>>;

using StoredTableNodePredicateNodePair = std::pair<std::shared_ptr<StoredTableNode>, std::shared_ptr<PredicateNode>>;

std::set<ChunkID> compute_chunk_exclude_list(const PredicatePruningChain& predicate_pruning_chain,
                                             const std::shared_ptr<StoredTableNode>& stored_table_node,
                                             std::unordered_map<StoredTableNodePredicateNodePair, std::set<ChunkID>,
                                              boost::hash<StoredTableNodePredicateNodePair>>& excluded_chunk_ids_by_predicate);

std::shared_ptr<TableStatistics> prune_table_statistics(const TableStatistics& old_statistics,
                                                                  OperatorScanPredicate predicate,
                                                                  size_t num_rows_pruned);



}  // namespace hyrise
