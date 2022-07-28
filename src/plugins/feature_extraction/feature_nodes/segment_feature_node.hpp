#pragma once

#include <unordered_map>

#include "abstract_feature_node.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

class SegmentFeatureNode : public AbstractFeatureNode {
 public:
  enum class Tier { Memory, HDD };

  SegmentFeatureNode(const Tier tier, const EncodingType encoding_type);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;

  Tier _tier;
  EncodingType _encoding_type;
};

}  // namespace opossum
