#pragma once

#include "feature_extraction/feature_types.hpp"

namespace opossum {

enum class FeatureNodeType { Operator, Predicate, AggregateFunction, Table, Column, Segment };

class AbstractFeatureNode : public std::enable_shared_from_this<AbstractFeatureNode> {
 public:
  AbstractFeatureNode(const FeatureNodeType type, const std::shared_ptr<AbstractFeatureNode>& left_input,
                      const std::shared_ptr<AbstractFeatureNode>& right_input = nullptr);

  virtual ~AbstractFeatureNode() = default;

  size_t hash() const;

  const FeatureVector& to_feature_vector();

  virtual const std::vector<std::string>& feature_headers() const = 0;

  std::shared_ptr<AbstractFeatureNode> left_input() const;
  std::shared_ptr<AbstractFeatureNode> right_input() const;

  FeatureNodeType type() const;

 protected:
  virtual std::shared_ptr<FeatureVector> _on_to_feature_vector() const = 0;

  virtual size_t _on_shallow_hash() const;

  FeatureNodeType _type;

  std::shared_ptr<AbstractFeatureNode> _left_input, _right_input;

  std::shared_ptr<FeatureVector> _feature_vector;
};

}  // namespace opossum
