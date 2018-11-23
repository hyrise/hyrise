#pragma once

#if __has_include(<filesystem>)
#include <filesystem>
namespace filesystem = std::filesystem;
#else
#include <experimental/filesystem>
namespace filesystem = std::experimental::filesystem;
#endif
