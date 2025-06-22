#pragma once

#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

class PlacementPlugin : public AbstractPlugin {
 public:
  PlacementPlugin() {}

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

  void run_page_placement(AbstractBenchmarkItemRunner& benchmark_item_runner);

  void run_column_placement(AbstractBenchmarkItemRunner& benchmark_item_runner);

  std::optional<PreBenchmarkHook> pre_benchmark_hook() final;

  std::optional<PostBenchmarkHook> post_benchmark_hook() final;

 private:
};

}  // namespace hyrise
