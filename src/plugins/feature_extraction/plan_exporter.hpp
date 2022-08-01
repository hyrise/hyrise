#pragma once

#include "feature_extraction/feature_nodes/abstract_feature_node.hpp"
#include "feature_extraction/feature_types.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class PlanExporter {
 public:
  void add_plan(const std::shared_ptr<Query>& query, const std::shared_ptr<const AbstractOperator>& pqp);

  void export_plans(const std::string& file_name);

 protected:
  void _features_to_csv(const std::string& query, const std::shared_ptr<AbstractFeatureNode>& graph,
                        std::unordered_map<FeatureNodeType, std::ofstream>& output_files,
                        const std::optional<size_t>& subquery = std::nullopt,
                        const std::optional<size_t>& subquery_id = std::nullopt) const;
  std::vector<std::shared_ptr<AbstractFeatureNode>> _feature_graphs;
};

}  // namespace opossum
