#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class TestPlugin : public AbstractPlugin {
 public:
  TestPlugin(const std::shared_ptr<HyriseEnvironmentRef>& init_hyrise_env) : AbstractPlugin(init_hyrise_env) {}

  std::string description() const final;

  void start() final;

  void stop() final;
};

}  // namespace opossum
