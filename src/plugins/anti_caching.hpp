#pragma once

#include "utils/abstract_plugin.hpp"

namespace opossum {

class AntiCaching : public AbstractPlugin {
 public:
  const std::string description() const;

  void start();

  void stop();

};

}  // namespace opossum
