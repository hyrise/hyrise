#pragma once

#include <unordered_map>

#include "abstract_feature_node.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

class SegmentFeatureNode : public AbstractFeatureNode {
 public:
  enum class Tier { Memory, HDD };

  SegmentFeatureNode(const Tier tier, ChunkOffset row_count, const std::unordered_map<EncodingType, uint64_t>& underlying_types);

  SegmentFeatureNode(const Tier tier, ChunkOffset row_count, const EncodingType encoding_type);

  static std::shared_ptr<SegmentFeatureNode> from_reference_segment(
      const std::shared_ptr<AbstractSegment>& reference_segment);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;

  Tier _tier;
  ChunkOffset _row_count;
  bool _is_reference = false;
  mutable std::unordered_map<EncodingType, uint64_t> _underlying_types;
};

}  // namespace opossum
