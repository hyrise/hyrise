#include "base_test.hpp"

#include "utils/log_manager.hpp"

namespace opossum {

class LogManagerTest : public BaseTest {
 protected:
  LogManager log_manager = LogManager{};
};

TEST_F(LogManagerTest, InsertMessage) {
  log_manager.add_message("foo", "bar", LogLevel::Info);
  EXPECT_EQ(log_manager.log_entries().size(), 1);
  const auto& entry = log_manager.log_entries()[0];
  EXPECT_EQ(entry.reporter, "foo");
  EXPECT_EQ(entry.message, "bar");
  EXPECT_EQ(entry.log_level, LogLevel::Info);
}

}  // namespace opossum
