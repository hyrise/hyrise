#include "column_feature_node.hpp"

#include <boost/container_hash/hash.hpp>

#include "expression/expression_utils.hpp"
#include "feature_extraction/feature_nodes/base_table_feature_node.hpp"
#include "feature_extraction/feature_nodes/operator_feature_node.hpp"
#include "feature_extraction/feature_nodes/result_table_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/base_value_segment.hpp"

namespace hyrise {

ColumnFeatureNode::ColumnFeatureNode(const bool is_reference, const std::shared_ptr<AbstractFeatureNode>& input_node,
                                     const ColumnID column_id,
                                     const std::shared_ptr<AbstractFeatureNode>& original_node,
                                     const ColumnID original_column_id)
    : AbstractFeatureNode{FeatureNodeType::Column, input_node}, _column_id{column_id}, _is_reference{is_reference} {
  const auto& input_table_node = static_cast<ResultTableFeatureNode&>(*input_node);
  _nullable = input_table_node.column_is_nullable(column_id);
  _data_type = input_table_node.column_data_type(column_id);
  _sorted_segments = input_table_node.sorted_segment_count(column_id);
  Assert(!input_table_node.registered_column(column_id), "Column already registered");

  const auto& base_table_node = static_cast<BaseTableFeatureNode&>(*original_node);
  const auto& table = base_table_node.table();
  const auto chunk_count = base_table_node.chunk_count();
  _segments.reserve(chunk_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    const auto& segment = chunk->get_segment(_column_id);
    const auto tier = SegmentFeatureNode::Tier::Memory;
    if (auto encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment)) {
      _segments.push_back(std::make_shared<SegmentFeatureNode>(tier, encoded_segment->encoding_type()));
    } else if (auto value_segment = std::dynamic_pointer_cast<BaseValueSegment>(segment)) {
      _segments.push_back(std::make_shared<SegmentFeatureNode>(tier, EncodingType::Unencoded));
    } else {
      Fail("Did not expect ReferenceSegment in base table");
    }
  }
}

ColumnFeatureNode::ColumnFeatureNode(const std::shared_ptr<AbstractFeatureNode>& input_node, const ColumnID column_id,
                                     const std::shared_ptr<AbstractFeatureNode>& original_node,
                                     const ColumnID original_column_id)
    : AbstractFeatureNode{FeatureNodeType::Column, input_node}, _column_id{column_id} {
  const auto& input_table_node = static_cast<ResultTableFeatureNode&>(*input_node);
  _is_reference = input_table_node.table_type() == TableType::References;
  _nullable = input_table_node.column_is_nullable(column_id);
  _data_type = input_table_node.column_data_type(column_id);
  _sorted_segments = input_table_node.sorted_segment_count(column_id);
  Assert(!input_table_node.registered_column(column_id), "Column already registered");

  const auto& result_table_node = static_cast<ResultTableFeatureNode&>(*original_node);
  Assert(
      result_table_node.table_type() == TableType::Data || result_table_node.column_is_materialized(original_column_id),
      "origininal column does not contain data");
  const auto& performance_data = result_table_node.performance_data();

  const auto chunk_count = performance_data.output_chunk_count;
  _segments.reserve(performance_data.output_chunk_count);

  // materialized segments are always ValueSegments in main memeory
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    _segments.push_back(
        std::make_shared<SegmentFeatureNode>(SegmentFeatureNode::Tier::Memory, EncodingType::Unencoded));
  }
}

std::shared_ptr<ColumnFeatureNode> ColumnFeatureNode::from_expression(
    const std::shared_ptr<AbstractFeatureNode>& operator_node, const std::shared_ptr<AbstractExpression>& expression,
    const ColumnID column_id) {
  const auto& [original_table, original_column_id] = find_original_column(expression, operator_node);

  Assert(operator_node->type() == FeatureNodeType::Operator, "expected operator node");
  const auto& input_table = static_cast<OperatorFeatureNode&>(*operator_node).output_table();
  const auto is_reference = input_table->table_type() == TableType::References;

  if (input_table->registered_column(column_id)) {
    return input_table->get_column(column_id);
  }

  std::shared_ptr<ColumnFeatureNode> column_node = nullptr;
  if (original_table->is_base_table()) {
    column_node =
        std::make_shared<ColumnFeatureNode>(is_reference, input_table, column_id, original_table, original_column_id);
  } else {
    column_node = std::make_shared<ColumnFeatureNode>(input_table, column_id, original_table, original_column_id);
  }

  input_table->register_column(column_node);
  return column_node;
}

size_t ColumnFeatureNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, _column_id);
  return hash;
}

std::shared_ptr<FeatureVector> ColumnFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<DataType>(_data_type);
  feature_vector->reserve(feature_vector->size() + 3);
  feature_vector->emplace_back(static_cast<Feature>(_nullable));
  feature_vector->emplace_back(static_cast<Feature>(_is_reference));
  feature_vector->emplace_back(static_cast<Feature::base_type>(_sorted_segments));

  return feature_vector;
}

const std::vector<std::string>& ColumnFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& ColumnFeatureNode::headers() {
  static auto ohe_headers_type = one_hot_headers<DataType>("data_type.");
  static const auto headers = std::vector<std::string>{"nullable", "is_reference", "sorted_segments"};
  if (ohe_headers_type.size() == magic_enum::enum_count<DataType>()) {
    ohe_headers_type.insert(ohe_headers_type.end(), headers.begin(), headers.end());
  }
  return ohe_headers_type;
}

ColumnID ColumnFeatureNode::column_id() const {
  return _column_id;
}

const std::vector<std::shared_ptr<SegmentFeatureNode>>& ColumnFeatureNode::segments() const {
  return _segments;
}

}  // namespace hyrise
