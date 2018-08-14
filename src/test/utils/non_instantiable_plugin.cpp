#include <iostream>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

// This plugin does not export its instantiation for testing purposes
class NonInstantiablePlugin : public AbstractPlugin, public Singleton<NonInstantiablePlugin> {
 public:
  const std::string description() const override {
    return "This is a not working Plugin because it does not export itself";
  }

  void start() const override {}

  void stop() const override {}
};

}  // namespace opossum
