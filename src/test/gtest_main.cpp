#if __has_include(<filesystem>)
#include <filesystem>
namespace filesystem = std::filesystem;
#else
#include <experimental/filesystem>
namespace filesystem = std::experimental::filesystem;
#endif

#include <gtest/gtest.h>
#include "base_test.hpp"

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

void create_test_data_directory(std::optional<std::string>& prefix) {
  Assert(!filesystem::exists(opossum::test_data_path),
         "Cannot create directory for test data: \"" + opossum::test_data_path + "\" already exists.");

  if (prefix) {
    Assert(filesystem::exists("./" + *prefix),
           "Cannot create directory for test data because \"" + *prefix + "\" does not exist");
  }

  filesystem::create_directory(opossum::test_data_path);
}

void remove_test_data_directory() {
  if (filesystem::exists(opossum::test_data_path)) {
    filesystem::remove_all(opossum::test_data_path);
  }
}

int main(int argc, char** argv) {
  Assert(filesystem::exists("src/test/tables"),
         "Cannot find src/test/tables. Are you running the test suite from the main folder of the Hyrise repository?");

  opossum::PerformanceWarningDisabler pwd;
  ::testing::InitGoogleTest(&argc, argv);

  std::optional<std::string> prefix;
  if (argc > 1) {
    // If argv[1] is set after gtest extracted its commands, we interpret it as directory name prefix for test data
    opossum::test_data_path = "./" + std::string(argv[1]) + "/.hyrise_test_data/";
    prefix = argv[1];
  } else {
    opossum::test_data_path = "./.hyrise_test_data/";
  }
  remove_test_data_directory();
  create_test_data_directory(prefix);

  int ret = RUN_ALL_TESTS();

  remove_test_data_directory();

  return ret;
}
