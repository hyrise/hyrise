#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace opossum {

// Removes whitespaces from the front and back. Also reduces multiple whitespaces between words to a single one.
// Splits the result by whitespace into single words. Intended for usage in the console.
std::vector<std::string> trim_and_split(const std::string& input);

std::vector<std::string> split_string_by_delimiter(const std::string& str, char delimiter);

// Returns the name of a plugin from its path. The name is the filename without the "lib" prefix and the file extension.
std::string plugin_name_from_path(const std::filesystem::path& path);

// Since CI pathes of source files can be quite long AND we want Assert-messages to be readable, we crop
// "/long/very/long/path/1234/src/lib/file.cpp" to "src/lib/file.cpp"
std::string trim_source_file_path(const std::string& path);

}  // namespace opossum
