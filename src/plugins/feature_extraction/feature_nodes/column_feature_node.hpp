#pragma once

#include "abstract_feature_node.hpp"
#include "expression/abstract_expression.hpp"
#include "feature_extraction/feature_nodes/abstract_feature_node.hpp"
#include "feature_extraction/feature_nodes/segment_feature_node.hpp"
#include "operators/operator_performance_data.hpp"

namespace hyrise {

class ColumnFeatureNode : public AbstractFeatureNode {
 public:
  ColumnFeatureNode(const bool is_reference, const std::shared_ptr<AbstractFeatureNode>& input_node,
                    const ColumnID column_id, const std::shared_ptr<AbstractFeatureNode>& original_node,
                    const ColumnID original_column_id);

  ColumnFeatureNode(const std::shared_ptr<AbstractFeatureNode>& input_node, const ColumnID column_id,
                    const std::shared_ptr<AbstractFeatureNode>& original_node, const ColumnID original_column_id);

  static std::shared_ptr<ColumnFeatureNode> from_expression(const std::shared_ptr<AbstractFeatureNode>& operator_node,
                                                            const std::shared_ptr<AbstractExpression>& expression,
                                                            const ColumnID column_id);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

  const std::vector<std::shared_ptr<SegmentFeatureNode>>& segments() const;

  ColumnID column_id() const;

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;
  size_t _on_shallow_hash() const final;

  ColumnID _column_id;
  DataType _data_type;

  ChunkID _chunk_count{0};
  bool _nullable{false};
  uint64_t _sorted_segments{0};
  bool _is_reference{false};

  std::vector<std::shared_ptr<SegmentFeatureNode>> _segments;
};

}  // namespace hyrise
