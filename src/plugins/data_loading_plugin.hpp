#pragma once

#include "storage/table.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

class DataLoadingPlugin : public AbstractPlugin {
 public:
  std::string description() const final;
  void start() final;
  void stop() final;
  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

 private:
  void _load_table_and_statistics();
  std::mutex _dbgen_mutex{};
  std::mutex _settings_mutex{};
  std::unordered_map<std::string, std::mutex> _table_mutexes{};
  std::vector<std::pair<std::string, ColumnID>> _loaded_columns{};

  std::chrono::microseconds _background_job_sleep_time{50};
};

}  // namespace hyrise
