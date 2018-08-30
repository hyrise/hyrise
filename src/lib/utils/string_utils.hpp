#include <string>

#include "utils/filesystem.hpp"

namespace opossum {

// Removes whitspaces from the front and back. Also reduces multiple whitespaces between words to a single one.
// Splits the result by whitespace into single words. Intended for usage in the console.
std::vector<std::string> trim_and_split(const std::string& input);

// Returns the name of a plugin from its path. The name is the filename without the "lib" prefix and the file extension.
const std::string plugin_name_from_path(const filesystem::path& path);

}  // namespace opossum
