#include "feature_extraction_plugin.hpp"

#include "feature_extraction/feature_types.hpp"
#include "hyrise.hpp"

namespace hyrise {

std::string FeatureExtractionPlugin::description() const {
  return "This is the Hyrise FeatureExtractionPlugin";
}

void FeatureExtractionPlugin::start() {
  _output_path = std::make_unique<OutputPath>("FeatureExtractionPlugin.OutputPath");
  _output_path->register_at_settings_manager();
  _query_exporter = std::make_unique<QueryExporter>();
  _plan_exporter = std::make_unique<PlanExporter>();
  _statistics_exporter = std::make_unique<StatisticsExporter>();
}

void FeatureExtractionPlugin::stop() {
  if (_worker_thread.joinable()) {
    _worker_thread.join();
    Assert(_output_path, "Output path was never set");
    Assert(_query_exporter, "QueryExporter was never set");
    Assert(_plan_exporter, "QueryExporter was never set");
    Assert(_statistics_exporter, "SatisticsExporter was never set");

    const auto& output_path = _output_path->get();
    std::filesystem::create_directories(output_path);
    _statistics_exporter->export_statistics(output_path);
    _query_exporter->export_queries(output_path);
    _plan_exporter->export_plans(output_path);
  } else {
    std::cout << "Feature extraction was never executed. Stopping plugin" << std::endl;
  }

  _output_path->unregister_at_settings_manager();
}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
FeatureExtractionPlugin::provided_user_executable_functions() {
  return {{"ExtractOperatorFeatures", [&]() { this->export_operator_features(); }}};
}

void FeatureExtractionPlugin::export_operator_features() {
  _worker_thread = std::thread{[&]() {
    std::cout << "export operator features" << std::endl;
    const auto& pqp_cache = Hyrise::get().default_pqp_cache;
    Assert(pqp_cache, "No PQPCache");

    const auto& cache_snapshot = pqp_cache->snapshot();
    for (const auto& [key, entry] : cache_snapshot) {
      const auto& pqp = entry.value;
      // Skip maintenance operators
      if (!operator_type_mapping.contains(pqp->type())) {
        continue;
      }

      const auto query = std::make_shared<Query>(key, *entry.frequency);
      _query_exporter->add_query(query);
      _plan_exporter->add_plan(query, pqp);
    }
  }};
}

FeatureExtractionPlugin::OutputPath::OutputPath(const std::string& init_name) : AbstractSetting(init_name) {}

const std::string& FeatureExtractionPlugin::OutputPath::description() const {
  static const auto description = std::string{"Output path for the Cost Model features"};
  return description;
}

const std::string& FeatureExtractionPlugin::OutputPath::get() {
  return _value;
}

void FeatureExtractionPlugin::OutputPath::set(const std::string& value) {
  _value = value;
}

EXPORT_PLUGIN(FeatureExtractionPlugin)

}  // namespace hyrise
