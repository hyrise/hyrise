#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"

using namespace opossum;  // NOLINT

int main(int argc, const char* argv[]) {
  if (argc > 1) {  // first argument is benchmark
    for (auto plugin_id = 1; plugin_id < argc; ++plugin_id) {
      const std::filesystem::path plugin_path(argv[plugin_id]);
      const auto plugin_name = plugin_name_from_path(plugin_path);
      Hyrise::get().plugin_manager.load_plugin(plugin_path);
    }
  }
  return 0;
}