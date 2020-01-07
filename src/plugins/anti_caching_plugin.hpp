#pragma once

#include <memory>

#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

class AntiCachingPlugin : public AbstractPlugin {
 public:
  const std::string description() const;

  void start();

  void stop();

 private:
  void _evaluate_statistics();

  std::unique_ptr<PausableLoopThread> _evaluate_statistics_thread;
  constexpr static std::chrono::milliseconds REFRESH_STATISTICS_INTERVAL = std::chrono::milliseconds(1000);
};

}  // namespace opossum
