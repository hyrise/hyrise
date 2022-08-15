#pragma once

#include "feature_extraction/plan_exporter.hpp"
#include "feature_extraction/query_exporter.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace hyrise {

class CostModelFeaturePlugin : public AbstractPlugin {
 public:
  CostModelFeaturePlugin() = default;

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

  void export_operator_features();

 protected:
  class OutputPath : public AbstractSetting {
   public:
    static constexpr const char* DEFAULT_OUTPUT_PATH = "./cost_model_features";
    explicit OutputPath(const std::string& init_name);

    const std::string& description() const final;

    const std::string& get() final;

    void set(const std::string& value) final;

   private:
    std::string _value = DEFAULT_OUTPUT_PATH;
  };

  std::shared_ptr<OutputPath> _output_path;
  std::shared_ptr<PlanExporter> _plan_exporter;
  std::shared_ptr<QueryExporter> _query_exporter;

  std::thread _worker_thread;
};

}  // namespace hyrise
