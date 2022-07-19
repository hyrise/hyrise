#pragma once

#include "feature_types.hpp"

namespace opossum {

enum class FeatureNodeType { Operator, Predicate, Table, Column, Segment };

class AbstractFeatureNode {
 public:

  AbstractFeatureNode(const FeatureNodeType type,  const std::shared_ptr<Query>& query, const std::shared_ptr<AbstractFeatureNode>& left_input, const std::shared_ptr<AbstractFeatureNode>& right_input = nullptr, const size_t run_time = 0);

  virtual ~AbstractFeatureNode() = default;

  virtual size_t hash() const = 0;

  size_t run_time() const;

  virtual const FeatureVector& to_feature_vector() const = 0;

  virtual const std::vector<std::string>& feature_headers() const = 0;

  std::shared_ptr<AbstractFeatureNode> left_input() const;
  std::shared_ptr<AbstractFeatureNode> right_input() const;

  std::shared_ptr<Query> query() const;

  FeatureNodeType type() const;

  bool is_operator() const;

 protected:
  FeatureNodeType _type;

  std::shared_ptr<Query> _query;

  std::shared_ptr<AbstractFeatureNode> _left_input, _right_input;

  size_t _run_time;

};

}  // namespace opossum
