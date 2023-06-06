#include <filesystem>
#include <fstream>
#include <sys/stat.h>

#include "base_test.hpp"

#include "utils/list_directory.hpp"

namespace hyrise {

class ListDirectoryTest : public BaseTest {};

TEST_F(ListDirectoryTest, ListBasicDirectory) {
  // Setup: Creates test-directory in the folder, the test is executed in.
  const auto TEST_DIRECTORY = std::string("./test-directory");

  struct stat s;
  auto directory_exists = (stat(TEST_DIRECTORY.c_str(), &s) == 0);
  if (!directory_exists) {
    directory_exists = std::filesystem::create_directory(TEST_DIRECTORY);
  }

  EXPECT_TRUE(directory_exists);

  const auto files = std::vector<std::string>{
    TEST_DIRECTORY + "/one.test",
    TEST_DIRECTORY + "/two.test",
    TEST_DIRECTORY + "/three.test",
    TEST_DIRECTORY + "/four.test"
  };

  for (const auto& file : files) {
    std::ofstream{ file };
  }
  std::filesystem::create_directory(TEST_DIRECTORY + "/test-subdirectory");

  const auto entries = list_directory(TEST_DIRECTORY);

  EXPECT_EQ(entries.size(), files.size());
  for (const auto& entry : entries) {
    const auto position = std::find(files.begin(), files.end(), entry);
    EXPECT_TRUE(position != files.end());
  }

  // Clean up
  std::filesystem::remove_all(TEST_DIRECTORY);
}

} // namespace hyrise
