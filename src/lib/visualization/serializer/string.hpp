/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#pragma once

#include <string>

namespace opossum {

// Crop a source file path to ensure readable assert messages (e.g., "/long/path/1234/src/lib/file.cpp" becomes
// "src/lib/file.cpp")
std::string TrimSourceFilePath(const std::string& path);

}  // namespace opossum
