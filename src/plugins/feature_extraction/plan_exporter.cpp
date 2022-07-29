#include "plan_exporter.hpp"

#include "feature_extraction/feature_nodes/operator_feature_node.hpp"

namespace opossum {

void PlanExporter::add_plan(const std::shared_ptr<Query>& query, const std::shared_ptr<const AbstractOperator>& pqp) {
  _feature_graphs.push_back(OperatorFeatureNode::from_pqp(pqp, query));
}

void PlanExporter::export_plans(const std::string& file_name) {
  std::cout << "export plans to " << file_name << std::endl;
}

}  // namespace opossum
