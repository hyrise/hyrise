#include <iostream>

#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

#define EXPORT(PluginName) \
extern "C" AbstractPlugin* factory() {\
  auto plugin = static_cast<AbstractPlugin*>(&(PluginName::get())); \
  return plugin; \
}

class AbstractPlugin : private Noncopyable {
protected:
  AbstractPlugin() {};
  AbstractPlugin& operator=(AbstractPlugin&&) = default;

public:

  static AbstractPlugin* get() {
    throw std::runtime_error("Every plugin needs to overwrite Plugin::get()");
  }

  virtual const std::string description() const = 0;

  virtual void start() const = 0;

  virtual void stop() const = 0;
};

}  // namespace opossum
