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
  std::mutex _tables_mutex{};
  std::mutex _columns_mutex{};
  std::mutex _histograms_mutex{};
  std::mutex _chunk_statistics_mutex{};

  std::vector<std::pair<std::pair<std::string, ColumnID>, std::string>> _columns{};
  std::vector<std::pair<std::string, std::string>> _tables{};

  std::unordered_map<std::string, std::shared_ptr<Table>> _table_cache;

  float _scale_factor{10.0};
};

}  // namespace hyrise
