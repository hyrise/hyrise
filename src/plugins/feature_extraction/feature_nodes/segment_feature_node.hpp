#pragma once

#include <unordered_map>

#include "abstract_feature_node.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"

namespace hyrise {

class SegmentFeatureNode : public AbstractFeatureNode {
 public:
  SegmentFeatureNode(const AbstractSegment::Tier tier, const EncodingType encoding_type,
                     const std::optional<CompressedVectorType>& compressed_vector_type = std::nullopt);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;

  AbstractSegment::Tier _tier;
  EncodingType _encoding_type;
  std::optional<CompressedVectorType> _compressed_vector_type;
};

}  // namespace hyrise
