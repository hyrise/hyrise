#pragma once

#include <memory>

#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "storage/table.hpp"

namespace opossum {

class AntiCachingPlugin : public AbstractPlugin {
 public:
  const std::string description() const;

  void start();

  void stop();

  static void export_access_statistics(const std::map<std::string, std::shared_ptr<Table>>& tables,
    const std::string& path_to_meta_data, const std::string& path_to_access_statistics);

  static void clear_access_statistics(const std::map<std::string, std::shared_ptr<Table>>& tables);

 private:
  void _evaluate_statistics();

  std::unique_ptr<PausableLoopThread> _evaluate_statistics_thread;
  constexpr static std::chrono::milliseconds REFRESH_STATISTICS_INTERVAL = std::chrono::milliseconds(1000);
};

}  // namespace opossum
