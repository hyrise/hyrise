#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_manager.hpp"
#include "hyrise.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/plugin_manager.hpp"
#include "./utils/plugin_test_utils.hpp"

namespace opossum {

class HyriseTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();
  }

  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() {
    auto& pm = Hyrise::get().plugin_manager;

    return pm._plugins;
  }
};

TEST_F(HyriseTest, GetAndResetHyrise) {
  auto& hyrise = Hyrise::get();

  EXPECT_EQ(get_plugins().size(), 0);
  hyrise.plugin_manager.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  EXPECT_EQ(get_plugins().size(), 1);

  EXPECT_EQ(hyrise.storage_manager.has_table("test_table"), false);
  const auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::Data);
  hyrise.storage_manager.add_table("test_table", table);
  EXPECT_EQ(hyrise.storage_manager.has_table("test_table"), true);

  EXPECT_EQ(hyrise.transaction_manager.last_commit_id(), CommitID{1});
  hyrise.transaction_manager.new_transaction_context()->commit();
  EXPECT_EQ(hyrise.transaction_manager.last_commit_id(), CommitID{2});

  Hyrise::reset();

  EXPECT_EQ(get_plugins().size(), 0);
  EXPECT_EQ(hyrise.storage_manager.has_table("test_table"), false);
  EXPECT_EQ(hyrise.transaction_manager.last_commit_id(), CommitID{1});

}

}
