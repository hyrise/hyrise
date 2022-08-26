#pragma once

#include "feature_extraction/plan_exporter.hpp"
#include "feature_extraction/query_exporter.hpp"
#include "feature_extraction/statistics_exporter.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace hyrise {

class FeatureExtractionPlugin : public AbstractPlugin {
 public:
  FeatureExtractionPlugin() = default;

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

  void export_operator_features();

 protected:
  class OutputPath : public AbstractSetting {
   public:
    static inline const std::string DEFAULT_OUTPUT_PATH{"./cost_model_features"};
    explicit OutputPath(const std::string& init_name);

    const std::string& description() const final;

    const std::string& get() final;

    void set(const std::string& value) final;

   private:
    std::string _value = DEFAULT_OUTPUT_PATH;
  };

  std::unique_ptr<OutputPath> _output_path;
  std::unique_ptr<PlanExporter> _plan_exporter;
  std::unique_ptr<QueryExporter> _query_exporter;
  std::unique_ptr<StatisticsExporter> _statistics_exporter;

  std::thread _worker_thread;
};

}  // namespace hyrise
