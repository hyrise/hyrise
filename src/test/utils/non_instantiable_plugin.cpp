#include <iostream>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/abstract_plugin.hpp"

namespace opossum {

class NonInstantiablePlugin : public AbstractPlugin {
  
public:
  static AbstractPlugin& get() {
    static NonInstantiablePlugin instance;
    return instance;
  }

  const std::string description() const override { 
    return "This is a not working Plugin because it does not export itself";
  }

  void start() const override {}

  void stop() const override {}
};

// EXPORT(TestPlugin)

}  // namespace opossum
