#include <iostream>

#define EXPORT(PluginName) \
extern "C" AbstractPlugin* factory(void) {\
  return static_cast<AbstractPlugin*>(&(PluginName::get()));\
}

namespace opossum {

class AbstractPlugin {
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

  virtual ~AbstractPlugin() {};
};

}  // namespace opossum
