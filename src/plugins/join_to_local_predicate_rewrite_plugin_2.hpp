#pragma once

#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

class JoinToLocalPredicateRewritePlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

  constexpr static std::chrono::milliseconds IDLE_DELAY_PREDICATE_REWRITE = std::chrono::milliseconds(10000);

 protected:
  void _start();

  std::unique_ptr<PausableLoopThread> _loop_thread_start;
};

}  // namespace opossum
