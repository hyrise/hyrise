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
#include "utils/settings/abstract_setting.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class CompressionPlugin : public AbstractPlugin {
 public:
  const std::string description() const final;

  void start() final;

  void stop() final;

 private:
  // Budget in megabyte
  class MemoryBudgetSetting : public AbstractSetting {
   public:
    explicit MemoryBudgetSetting() : AbstractSetting("CompressionPlugin_MemoryBudget", "5000") {}
    const std::string& description() const final {
      static const auto description = std::string{"The memory budget to target for the CompressionPlugin."};
      return description;
    }
    const std::string& get() { return _value; }
    void set(const std::string& value) final { _value = value; }
  };

  constexpr static std::chrono::milliseconds THREAD_INTERVAL = std::chrono::milliseconds(7'500);

  std::unique_ptr<PausableLoopThread> _loop_thread;

  void _optimize_compression();

  bool _optimized = false;

  std::shared_ptr<MemoryBudgetSetting> _memory_budget_setting;
};

}  // namespace opossum
