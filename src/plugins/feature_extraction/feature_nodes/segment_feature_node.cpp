#include "segment_feature_node.hpp"

#include <magic_enum.hpp>
#include <numeric>

#include "abstract_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

SegmentFeatureNode::SegmentFeatureNode(const Tier tier, const EncodingType encoding_type)
    : AbstractFeatureNode{FeatureNodeType::Segment, nullptr}, _tier{tier}, _encoding_type{encoding_type} {}

const std::vector<std::string>& SegmentFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& SegmentFeatureNode::headers() {
  static auto ohe_headers_tier = one_hot_headers<Tier>("tier.");
  static const auto ohe_headers_encoding = one_hot_headers<EncodingType>("encoding_type.");
  if (ohe_headers_tier.size() == magic_enum::enum_count<Tier>()) {
    ohe_headers_tier.insert(ohe_headers_tier.end(), ohe_headers_encoding.begin(), ohe_headers_encoding.end());
  }
  return ohe_headers_tier;
}

std::shared_ptr<FeatureVector> SegmentFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<Tier>(_tier);
  const auto encoding_types = one_hot_encoding<EncodingType>(_encoding_type);
  feature_vector->reserve(feature_vector->size() + encoding_types->size());
  feature_vector->insert(feature_vector->end(), encoding_types->begin(), encoding_types->end());

  return feature_vector;
}

}  // namespace opossum
