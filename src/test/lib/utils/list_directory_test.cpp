#include <filesystem>
#include <fstream>
#include <unordered_set>
#include <limits>

#include "base_test.hpp"

#include "utils/list_directory.hpp"

namespace hyrise {

class ListDirectoryTest : public BaseTest {
 protected:
  void SetUp() override {
    std::filesystem::create_directory(_directory_path);
  }

  void TearDown() override {
    std::filesystem::remove_all(_directory_path);
  }

  std::string _directory_path = test_data_path + "/test-directory";
};

TEST_F(ListDirectoryTest, ListDirectoryWithFilesAndSubdirectory) {
  const auto files = std::unordered_set<std::string>{_directory_path + "/one.test", _directory_path + "/two.test",
                                                     _directory_path + "/three.test", _directory_path + "/four.test"};

  for (const auto& file : files) {
    std::ofstream{file};
  }

  std::filesystem::create_directory(_directory_path + "/test-subdirectory");

  const auto entries = list_directory(_directory_path);

  EXPECT_EQ(entries.size(), files.size());
  for (const auto& entry : entries) {
    EXPECT_TRUE(files.contains(entry));
  }
}

TEST_F(ListDirectoryTest, ListEmptyDirectory) {
  EXPECT_EQ(list_directory(_directory_path).size(), 0);
}

TEST_F(ListDirectoryTest, ListDirectoryWithFilesOnly) {
  const auto files = std::unordered_set<std::string>{_directory_path + "/one.test", _directory_path + "/two.test",
                                                     _directory_path + "/three.test", _directory_path + "/four.test"};
  for (const auto& file : files) {
    std::ofstream{file};
  }

  const auto entries = list_directory(_directory_path);

  EXPECT_EQ(entries.size(), files.size());
  for (const auto& entry : entries) {
    EXPECT_TRUE(files.contains(entry));
  }
}

TEST_F(ListDirectoryTest, ListDirectoryWithSubdirectoriesOnly) {
  std::filesystem::create_directory(_directory_path + "/subdir1");
  std::filesystem::create_directory(_directory_path + "/subdir2");
  std::filesystem::create_directory(_directory_path + "/subdir3");

  const auto entries = list_directory(_directory_path);
  EXPECT_EQ(entries.size(), 0);
}

}  // namespace hyrise
