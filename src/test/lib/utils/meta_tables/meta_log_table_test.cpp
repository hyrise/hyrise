#include <regex>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "utils/meta_tables/meta_log_table.hpp"

namespace hyrise {

class MetaLogTest : public BaseTest {
 protected:
  void SetUp() override {
    meta_log_table = std::make_shared<MetaLogTable>();
    Hyrise::get().log_manager.add_message("foo", "bar", LogLevel::Info);
  }

  const std::shared_ptr<Table> generate_meta_table() const {
    return meta_log_table->_on_generate();
  }

  std::shared_ptr<MetaLogTable> meta_log_table;
};

TEST_F(MetaLogTest, IsImmutable) {
  EXPECT_FALSE(meta_log_table->can_insert());
  EXPECT_FALSE(meta_log_table->can_update());
  EXPECT_FALSE(meta_log_table->can_delete());
}

TEST_F(MetaLogTest, TableGeneration) {
  const auto column_definitions =
      TableColumnDefinitions{{"timestamp", DataType::Long, false},   {"time", DataType::String, false},
                             {"log_level", DataType::String, false}, {"log_level_id", DataType::Int, false},
                             {"reporter", DataType::String, false},  {"message", DataType::String, false}};
  EXPECT_EQ(meta_log_table->column_definitions(), column_definitions);

  const auto timestamp_ns = std::chrono::nanoseconds{std::chrono::system_clock::now().time_since_epoch()}.count();

  const auto meta_table = generate_meta_table();
  EXPECT_EQ(meta_table->row_count(), 1);

  const auto values = meta_table->get_row(0);

  // Log entry should be created less than a minute ago but before the meta table was generated.
  EXPECT_GT(boost::get<int64_t>(values[0]), timestamp_ns - static_cast<int64_t>(60e9));
  // Since the clock is not necessarily precise enough, we use "less or equals" instead of "less than" here.
  EXPECT_LE(boost::get<int64_t>(values[0]), timestamp_ns);

  // We only check if the human-readable timestamp has the format YYYY-MM-DD HH:MM:SS.
  const auto timestamp_regex = std::regex{"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"};
  auto match = std::smatch{};
  const auto timestamp_string = std::string{boost::get<pmr_string>(values[1])};
  EXPECT_TRUE(std::regex_match(timestamp_string, match, timestamp_regex));

  EXPECT_EQ(values[2], AllTypeVariant{pmr_string{"Info"}});
  EXPECT_EQ(values[3], AllTypeVariant{static_cast<int32_t>(1)});
  EXPECT_EQ(values[4], AllTypeVariant{pmr_string{"foo"}});
  EXPECT_EQ(values[5], AllTypeVariant{pmr_string{"bar"}});
}

}  // namespace hyrise
