#pragma once

#include <algorithm>
#include <mutex>
#include <numeric>
#include <queue>
#include <thread>

#include "gtest/gtest_prod.h"
#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class ClusteringPlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

 private:
  constexpr static std::chrono::milliseconds THREAD_INTERVAL = std::chrono::milliseconds(1'000);

  std::unique_ptr<PausableLoopThread> _loop_thread;

  void _optimize_clustering();

  bool _optimized = false;
};

}  // namespace opossum
