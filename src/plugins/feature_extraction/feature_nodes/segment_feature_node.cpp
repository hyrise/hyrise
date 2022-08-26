#include "segment_feature_node.hpp"

#include <magic_enum.hpp>

#include "feature_extraction/feature_extraction_utils.hpp"

namespace hyrise {

SegmentFeatureNode::SegmentFeatureNode(const AbstractSegment::Tier tier, const EncodingType encoding_type,
                                       const bool sorted,
                                       const std::optional<CompressedVectorType>& compressed_vector_type)
    : AbstractFeatureNode{FeatureNodeType::Segment, nullptr},
      _tier{tier},
      _encoding_type{encoding_type},
      _sorted{sorted},
      _compressed_vector_type{compressed_vector_type} {}

const std::vector<std::string>& SegmentFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& SegmentFeatureNode::headers() {
  static auto ohe_headers_tier = one_hot_headers<AbstractSegment::Tier>("tier.");
  static const auto ohe_headers_encoding = one_hot_headers<EncodingType>("encoding_type.");
  static const auto ohe_headers_vector = one_hot_headers<CompressedVectorType>("compressed_vector_type.");
  if (ohe_headers_tier.size() == magic_enum::enum_count<AbstractSegment::Tier>()) {
    ohe_headers_tier.reserve(ohe_headers_tier.size() + ohe_headers_encoding.size() + ohe_headers_vector.size() + 1);
    ohe_headers_tier.insert(ohe_headers_tier.end(), ohe_headers_encoding.cbegin(), ohe_headers_encoding.cend());
    ohe_headers_tier.emplace_back("sorted");
    ohe_headers_tier.insert(ohe_headers_tier.end(), ohe_headers_vector.cbegin(), ohe_headers_vector.cend());
  }
  return ohe_headers_tier;
}

std::shared_ptr<FeatureVector> SegmentFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<AbstractSegment::Tier>(_tier);
  const auto encoding_types = one_hot_encoding<EncodingType>(_encoding_type);
  const auto& vector_compression = _compressed_vector_type
                                       ? *one_hot_encoding<CompressedVectorType>(*_compressed_vector_type)
                                       : FeatureVector(magic_enum::enum_count<CompressedVectorType>());

  feature_vector->reserve(headers().size());
  feature_vector->insert(feature_vector->end(), encoding_types->cbegin(), encoding_types->cend());
  feature_vector->emplace_back(static_cast<Feature>(_sorted));
  feature_vector->insert(feature_vector->end(), vector_compression.cbegin(), vector_compression.cend());

  return feature_vector;
}

}  // namespace hyrise
