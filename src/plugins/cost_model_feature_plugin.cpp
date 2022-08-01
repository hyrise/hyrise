#include "cost_model_feature_plugin.hpp"

#include "feature_extraction/feature_types.hpp"
#include "hyrise.hpp"

namespace opossum {

std::string CostModelFeaturePlugin::description() const {
  return "This is the Hyrise CostModelFeaturePlugin";
}

void CostModelFeaturePlugin::start() {
  _output_path = std::make_shared<OutputPath>("hyriseCostModelFeaturePlugin.OutputPath");
  _output_path->register_at_settings_manager();
  _query_exporter = std::make_shared<QueryExporter>();
  _plan_exporter = std::make_shared<PlanExporter>();
}

void CostModelFeaturePlugin::stop() {
  Assert(_output_path, "Output path was never set");
  Assert(_query_exporter, "QueryExporter was never set");
  Assert(_plan_exporter, "QueryExporter was never set");

  std::filesystem::create_directories(_output_path->get());
  _output_path->unregister_at_settings_manager();
  _query_exporter->export_queries(_output_path->get());
  _plan_exporter->export_plans(_output_path->get());
}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
CostModelFeaturePlugin::provided_user_executable_functions() const {
  return {{"ExtractOperatorFeatures", [&]() { this->export_operator_features(); }}};
}

void CostModelFeaturePlugin::export_operator_features() const {
  std::cout << "export operator features" << std::endl;
  const auto& pqp_cache = Hyrise::get().default_pqp_cache;
  Assert(pqp_cache, "No PQPCache");

  const auto non_selection_operators = std::unordered_set<OperatorType>{OperatorType::ChangeMetaTable,
                                                                        OperatorType::CreateTable,
                                                                        OperatorType::CreatePreparedPlan,
                                                                        OperatorType::CreateView,
                                                                        OperatorType::DropTable,
                                                                        OperatorType::DropView,
                                                                        OperatorType::Delete,
                                                                        OperatorType::Export,
                                                                        OperatorType::Import,
                                                                        OperatorType::Insert,
                                                                        OperatorType::JoinVerification,
                                                                        OperatorType::Print,
                                                                        OperatorType::Update,
                                                                        OperatorType::Mock};
  const auto& cache_snapshot = pqp_cache->snapshot();
  for (const auto& [key, entry] : cache_snapshot) {
    const auto& pqp = entry.value;
    // Skip maintenance operators
    if (non_selection_operators.contains(pqp->type())) {
      continue;
    }

    const auto& query_hash = QueryExporter::query_hash(key);
    const auto query = std::make_shared<Query>(query_hash, key, *entry.frequency);
    _query_exporter->add_query(query);
    _plan_exporter->add_plan(query, pqp);
  }
}

CostModelFeaturePlugin::OutputPath::OutputPath(const std::string& init_name) : AbstractSetting(init_name) {}

const std::string& CostModelFeaturePlugin::OutputPath::description() const {
  static const auto description = std::string{"Output path for the Cost Model features"};
  return description;
}

const std::string& CostModelFeaturePlugin::OutputPath::get() {
  return _value;
}

void CostModelFeaturePlugin::OutputPath::set(const std::string& value) {
  _value = value;
}

EXPORT_PLUGIN(CostModelFeaturePlugin)

}  // namespace opossum
