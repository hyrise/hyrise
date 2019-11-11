#pragma once

#include <filesystem>
#include <vector>

namespace opossum {

// This is a hack to work around https://gcc.gnu.org/bugzilla/show_bug.cgi?id=91067
// By putting the iterator into its own compilation unit, we can set its optimization level to -O0. This hides the bug.
// TODO(anyone): Replace callers with the directory_iterator once gcc 9.3 fixes the stdlibc++ bug.
std::vector<std::filesystem::path> list_directory(const std::string& directory);

}  // namespace opossum
