#include "base_test.hpp"

#include "hyrise.hpp"
#include "utils/meta_tables/meta_log_table.hpp"

namespace opossum {

class MetaLogTest : public BaseTest {
 protected:
  void SetUp() {
    meta_log_table = std::make_shared<MetaLogTable>();
    Hyrise::get().log_manager.add_message("foo", "bar", LogLevel::Info);
  }

  void TearDown() { Hyrise::reset(); }

  const std::shared_ptr<Table> generate_meta_table() const { return meta_log_table->_on_generate(); }

  std::shared_ptr<MetaLogTable> meta_log_table;
};

TEST_F(MetaLogTest, IsImmutable) {
  EXPECT_FALSE(meta_log_table->can_insert());
  EXPECT_FALSE(meta_log_table->can_update());
  EXPECT_FALSE(meta_log_table->can_delete());
}

TEST_F(MetaLogTest, TableGeneration) {
  const auto column_definitions = TableColumnDefinitions{{"timestamp", DataType::String, false},
                                                         {"log_level", DataType::String, false},
                                                         {"log_level_id", DataType::Int, false},
                                                         {"reporter", DataType::String, false},
                                                         {"message", DataType::String, false}};
  EXPECT_EQ(meta_log_table->column_definitions(), column_definitions);

  const auto meta_table = generate_meta_table();
  EXPECT_EQ(meta_table->row_count(), 1);

  const auto values = meta_table->get_row(0);
  EXPECT_EQ(values[1], AllTypeVariant{pmr_string{"Info"}});
  EXPECT_EQ(values[2], AllTypeVariant{static_cast<int32_t>(1)});
  EXPECT_EQ(values[3], AllTypeVariant{pmr_string{"foo"}});
  EXPECT_EQ(values[4], AllTypeVariant{pmr_string{"bar"}});
}

}  // namespace opossum
