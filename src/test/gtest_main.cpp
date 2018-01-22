#if __has_include(<filesystem>)
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

#if __has_include(<filesystem>)
namespace filesystem = std::filesystem;
#else
namespace filesystem = std::experimental::filesystem;
#endif

#include <gtest/gtest.h>
#include "base_test.hpp"

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

void create_test_data_directory(std::optional<std::string>& prefix) {
  if (prefix)
    opossum::test_data_path = "./" + *prefix + "/test_data/";
  else
    opossum::test_data_path = "./test_data/";

  if (filesystem::exists(opossum::test_data_path)) {
    filesystem::remove_all(opossum::test_data_path);
  }
  filesystem::create_directories(opossum::test_data_path);
}

void remove_test_data_directory(std::optional<std::string>& prefix) {
  if (prefix)
    filesystem::remove_all("./" + *prefix);
  else
    filesystem::remove_all(opossum::test_data_path);
}

int main(int argc, char** argv) {
  opossum::Assert(
      filesystem::exists("src/test/tables"),
      "Cannot find src/test/tables. Are you running the test suite from the main folder of the Hyrise repository?");

  opossum::PerformanceWarningDisabler pwd;
  ::testing::InitGoogleTest(&argc, argv);

  std::optional<std::string> test_data_prefix;
  if (argc > 1) {
    // If argv[1] is set after gtest extracted its commands, we interpret it as directory name prefix for test data
    test_data_prefix = argv[1];
  }
  create_test_data_directory(test_data_prefix);

  int ret = RUN_ALL_TESTS();

  remove_test_data_directory(test_data_prefix);

  return ret;
}
