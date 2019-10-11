#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "storage/index/index_statistics.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

/**
 * This optimizer rule finds PredicateNodes whose inputs are StoredTableNodes. These PredicateNodes are candidates
 * for being executed by IndexScans. If the expected selectivity of the predicate falls below a certain threshold, the
 * ScanType of the PredicateNode is set to IndexScan.
 *
 * Note:
 * For now this rule is only applicable to single-column indexes. Multi-column predicates (i.e. WHERE a < b) are also
 * not supported. We also assume that if chunks have an index, all of them are of the same type, we do not mix GroupKey
 * and ART indexes. In addition, chains of IndexScans are not possible since an IndexScan's input must be a GetTable.
 * Currently, only GroupKeyIndexes are supported.
 */

class IndexScanRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

 protected:
  bool _is_index_scan_applicable(const IndexStatistics& index_statistics,
                                 const std::shared_ptr<PredicateNode>& predicate_node) const;
  inline bool _is_single_segment_index(const IndexStatistics& index_statistics) const;
};

}  // namespace opossum
