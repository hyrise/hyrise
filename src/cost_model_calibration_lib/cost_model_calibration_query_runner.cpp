#include "cost_model_calibration_query_runner.hpp"

#include "calibration_feature_extractor.hpp"
#include "concurrency/transaction_manager.hpp"
#include "hyrise.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/format_duration.hpp"

namespace opossum {

CostModelCalibrationQueryRunner::CostModelCalibrationQueryRunner(const CalibrationConfiguration configuration)
    : _configuration(configuration) {}

const std::vector<cost_model::CostModelFeatures> CostModelCalibrationQueryRunner::calibrate_query_from_lqp(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // lqp->print();
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);

  LQPTranslator lqp_translator{};
  const auto pqp = lqp_translator.translate_node(lqp);
  pqp->set_transaction_context_recursively(transaction_context);
  const auto tasks = OperatorTask::make_tasks_from_operator(pqp);

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  return _evaluate_query_plan({pqp});
}

const std::vector<cost_model::CostModelFeatures> CostModelCalibrationQueryRunner::calibrate_query_from_sql(
    const std::string& query) const {
  std::cout << query << std::endl;

  //  SQLQueryCache<SQLQueryPlan>::get().clear();

  auto pipeline_builder = SQLPipelineBuilder{query};
  pipeline_builder.disable_mvcc();
  auto pipeline = pipeline_builder.create_pipeline();

  // Execute the query, we don't care about the results
  pipeline.get_result_table();

  const auto pqps = pipeline.get_physical_plans();
  return _evaluate_query_plan(pqps);
}

const std::vector<cost_model::CostModelFeatures> CostModelCalibrationQueryRunner::_evaluate_query_plan(
    const std::vector<std::shared_ptr<AbstractOperator>>& pqps) const {
  std::vector<cost_model::CostModelFeatures> features{};
  for (const auto& pqp : pqps) {
    _traverse(pqp, features);
  }

  return features;
}

void CostModelCalibrationQueryRunner::_traverse(const std::shared_ptr<const AbstractOperator>& op,
                                                std::vector<cost_model::CostModelFeatures>& features) const {
  if (op->left_input() != nullptr) {
    _traverse(op->left_input(), features);
  }

  if (op->right_input() != nullptr) {
    _traverse(op->right_input(), features);
  }


  const auto operator_result = cost_model::CalibrationFeatureExtractor::extract_features(op);
  if (operator_result)
    features.push_back(*operator_result);
}

}  // namespace opossum
