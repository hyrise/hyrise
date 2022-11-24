#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace hyrise {

// This plugin does not export its instantiation so that we can test if this case is handled correctly.
// NOLINTNEXTLINE(fuchsia-multiple-inheritance)
class TestNonInstantiablePlugin : public AbstractPlugin, public Singleton<TestNonInstantiablePlugin> {
 public:
  std::string description() const final {
    return "This is a not working Plugin because it does not export itself";
  }

  void start() final {}

  void stop() final {}
};

}  // namespace hyrise
