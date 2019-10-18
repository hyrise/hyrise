#include "list_directory.hpp"

namespace opossum {

std::vector<std::filesystem::path> list_directory(const std::string& directory) {
  std::vector<std::filesystem::path> files;

  for (const auto& directory_entry : std::filesystem::recursive_directory_iterator(directory)) {
    if (!std::filesystem::is_regular_file(directory_entry)) continue;

    files.emplace_back(directory_entry.path());
  }

  return files;
}

}  // namespace opossum
