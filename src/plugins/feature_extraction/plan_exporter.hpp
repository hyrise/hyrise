#pragma once

#include <fstream>

#include "feature_extraction/feature_nodes/abstract_feature_node.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "feature_extraction/feature_types.hpp"
#include "operators/abstract_operator.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace hyrise {

class PlanExporter {
 public:
  void add_plan(const std::shared_ptr<Query>& query, const std::shared_ptr<const AbstractOperator>& pqp);

  void export_plans(const std::string& file_name);

 protected:
  void _features_to_csv(const std::string& query, const std::shared_ptr<AbstractFeatureNode>& graph,
                        CardinalityEstimator& cardinality_estimator,
                        const std::optional<size_t>& subquery = std::nullopt,
                        const std::optional<size_t>& subquery_id = std::nullopt);

  void _export_column(ColumnFeatureNode& column_node, const std::string& prefix, const size_t column_id);

  std::vector<std::shared_ptr<AbstractFeatureNode>> _feature_graphs{};
  std::unordered_map<FeatureNodeType, std::ofstream> _output_files{};
};

}  // namespace hyrise
