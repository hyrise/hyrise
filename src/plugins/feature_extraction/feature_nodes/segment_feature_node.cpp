#include "segment_feature_node.hpp"

#include <numeric>
#include <magic_enum.hpp>

#include "abstract_feature_node.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace opossum {



  SegmentFeatureNode::SegmentFeatureNode(const Tier tier, ChunkOffset row_count, const std::unordered_map<EncodingType, uint64_t>& underlying_types) : AbstractFeatureNode{FeatureNodeType::Segment, nullptr}, _tier{tier}, _row_count{row_count},  _is_reference{true}, _underlying_types{underlying_types} {}

  SegmentFeatureNode::SegmentFeatureNode(const Tier tier, ChunkOffset row_count, const EncodingType encoding_type)  : AbstractFeatureNode{FeatureNodeType::Segment, nullptr}, _tier{tier}, _row_count{row_count},  _is_reference{false}, _underlying_types{{encoding_type, 1}} {}


  std::shared_ptr<SegmentFeatureNode> SegmentFeatureNode::from_reference_segment(
      const std::shared_ptr<AbstractSegment>& reference_segment) {
    return nullptr;
  }

  const std::vector<std::string>& SegmentFeatureNode::feature_headers() const {
  return headers();
  }

  const std::vector<std::string>& SegmentFeatureNode::headers() {
    static auto ohe_headers_tier = one_hot_headers<Tier>("tier.");
    static const auto ohe_headers_encoding = one_hot_headers<EncodingType>("underlying_type.");
    static const auto headers = std::vector<std::string>{"row_count",  "is_reference"};
  if (ohe_headers_tier.size() == magic_enum::enum_count<Tier>()) {
    ohe_headers_tier.insert(ohe_headers_tier.end(), headers.begin(), headers.end());
    ohe_headers_tier.insert(ohe_headers_tier.end(), ohe_headers_encoding.begin(), ohe_headers_encoding.end());
  }
  return ohe_headers_tier;

  }

std::shared_ptr<FeatureVector> SegmentFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<Tier>(_tier);
  const auto encoding_types = magic_enum::enum_values<EncodingType>();
  feature_vector->reserve(_feature_vector->size() + 2 + encoding_types.size());
  feature_vector->emplace_back(static_cast<Feature>(_row_count));
  feature_vector->emplace_back(static_cast<Feature>(_is_reference));
  const auto num_referenced_segments = std::accumulate(_underlying_types.cbegin(), _underlying_types.cend(), Feature{0}, [](const auto previous, const auto& entry){ return previous + static_cast<Feature>(entry.second); });
  Assert(num_referenced_segments > 0, "any segment type has to be set");
  for (const auto& encoding_type : encoding_types) {
    feature_vector->emplace_back(static_cast<Feature>(_underlying_types[encoding_type]) / num_referenced_segments);
  }

  return feature_vector;
}


}  // namespace opossum
