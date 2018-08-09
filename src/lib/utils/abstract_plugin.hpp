#include <iostream>

#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

struct Injection {
  StorageManager* const storage_manager;
};

#define EXPORT(PluginName) \
extern "C" AbstractPlugin* factory(Injection injection) {\
  auto plugin = static_cast<AbstractPlugin*>(&(PluginName::get())); \
  plugin->inject(injection); \
  return plugin;\
}

class AbstractPlugin : private Noncopyable {
protected:
  AbstractPlugin() {};
  AbstractPlugin& operator=(AbstractPlugin&&) = default;

  StorageManager* _storage_manager;

public:

  static AbstractPlugin* get() {
    throw std::runtime_error("Every plugin needs to overwrite Plugin::get()");
  }

  virtual const std::string description() const = 0;

  virtual void start() const = 0;

  virtual void stop() const = 0;

  void inject(Injection injection) {
    _storage_manager = injection.storage_manager;
  }
};

}  // namespace opossum
