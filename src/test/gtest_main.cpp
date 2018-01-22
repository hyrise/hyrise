#if __has_include(<filesystem>)
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

#include <gtest/gtest.h>
#include "base_test.hpp"

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

#if __has_include(<filesystem>)
namespace filesystem = std::filesystem;
#else
namespace filesystem = std::experimental::filesystem;
#endif

void create_test_data_directory(std::optional<std::string>& prefix) {
  if (prefix)
    opossum::TEST_DATA_PATH = "./" + *prefix + "/test_data/";
  else
    opossum::TEST_DATA_PATH = "./test_data/";

  if (filesystem::exists(opossum::TEST_DATA_PATH)) {
    filesystem::remove_all(opossum::TEST_DATA_PATH);
  }
  filesystem::create_directories(opossum::TEST_DATA_PATH);
}

void remove_test_data_directory(std::optional<std::string>& prefix) {
  if (prefix)
    filesystem::remove_all("./" + *prefix);
  else
    filesystem::remove_all(opossum::TEST_DATA_PATH);
}

int main(int argc, char** argv) {
  opossum::Assert(filesystem::exists("src/test/tables"),
      "Cannot find src/test/tables. Are you running the test suite from the main folder of the Hyrise repository?");

  std::optional<std::string> test_data_prefix;
  if (argc > 1) {
    // If argv[1] is not a gtest command, we interpret it as directory name prefix for test data files
    if (std::string(argv[1]).find("--") == std::string::npos) {
      test_data_prefix = argv[1];
    }
  }
  create_test_data_directory(test_data_prefix);

  opossum::PerformanceWarningDisabler pwd;
  ::testing::InitGoogleTest(&argc, argv);

  int ret = RUN_ALL_TESTS();

  remove_test_data_directory(test_data_prefix);

  return ret;
}
