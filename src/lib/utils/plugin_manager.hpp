#include <dlfcn.h>
#include <experimental/filesystem>

#include "utils/assert.hpp"

namespace opossum {
class PluginManager {
 public:
  std::vector<void*> plugins;

  static PluginManager& get() {
    // This is not in a separate file to keep the PoC short.
    static PluginManager instance;
    return instance;
  }

  void initialize() {
    for (auto& file : std::experimental::filesystem::directory_iterator("plugins")) {
      std::cout << "Found plugin at " << file << " - ";

      // dynamically load the plugin by path, resolving all symbols immediately but not making them
      // visible to anyone else
      auto plugin = dlopen(file.path().string().c_str(), RTLD_NOW | RTLD_LOCAL);
      DebugAssert(plugin, "Loading plugin failed: " + dlerror());
      plugins.push_back(plugin);

      // say hello
      auto name_method = (char* (*)())dlsym(plugin, "name");
      DebugAssert(name_method, std::string("Resolving name() failed: ") + dlerror());
      std::cout << "started " << (*name_method)() << std::endl;
    }
  }

  ~PluginManager() {
    for (auto plugin : plugins) {
      dlclose(plugin);
    }
  }
};
}  // namespace opossum