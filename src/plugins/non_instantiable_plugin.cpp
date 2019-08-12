#include <iostream>

#include "storage/table.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

// This plugin does not export its instantiation so that we can test if this case is handled correctly.
class TestNonInstantiablePlugin : public AbstractPlugin, public Singleton<TestNonInstantiablePlugin> {
 public:
  const std::string description() const final {
    return "This is a not working Plugin because it does not export itself";
  }

  void start() final {}

  void stop() final {}
};

}  // namespace opossum
