#pragma once

#include "utils/abstract_plugin.hpp"

namespace opossum {

class LQPParsePlugin : public AbstractPlugin {
 public:

  std::string description() const final;

  void start() final;

  void stop() final;
};

}  // namespace opossum
