#pragma once

#include "feature_extraction/feature_types.hpp"

namespace opossum {

enum class FeatureNodeType { Operator, Predicate, AggregateFunction, Table, Column, Segment };

class AbstractFeatureNode : public std::enable_shared_from_this<AbstractFeatureNode> {
 public:

  AbstractFeatureNode(const FeatureNodeType type, const std::shared_ptr<AbstractFeatureNode>& left_input, const std::shared_ptr<AbstractFeatureNode>& right_input = nullptr, const std::chrono::nanoseconds& run_time = std::chrono::nanoseconds{0}, const std::shared_ptr<Query>& query = nullptr);

  virtual ~AbstractFeatureNode() = default;

  virtual size_t hash() const = 0;

  std::chrono::nanoseconds run_time() const;

  const FeatureVector& to_feature_vector();

  virtual const std::vector<std::string>& feature_headers() const = 0;

  std::shared_ptr<AbstractFeatureNode> left_input() const;
  std::shared_ptr<AbstractFeatureNode> right_input() const;

  std::shared_ptr<Query> query() const;

  FeatureNodeType type() const;

  bool is_operator() const;

  bool is_root_node() const;

  void set_as_root_node();

 protected:
  virtual std::shared_ptr<FeatureVector> _on_to_feature_vector() = 0;

  FeatureNodeType _type;

  std::shared_ptr<AbstractFeatureNode> _left_input, _right_input;

  std::chrono::nanoseconds _run_time;

  std::shared_ptr<Query> _query;

  bool _is_root_node = false;

  std::shared_ptr<FeatureVector> _feature_vector;

};

}  // namespace opossum
