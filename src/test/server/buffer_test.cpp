// #include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "server_without_boost/buffer.hpp"

namespace opossum {

class BufferTest : public BaseTest {
 protected:
  Buffer buffer;
};

TEST_F(BufferTest, WriteMoreThanAvailable) {
  const std::string numbers = "0123456789";

  auto index = 0;
  while (index < 140) {
    buffer.insert_value(numbers, false);
    index++;
  }
  index = 0;
  buffer.reset();
  while (index < 4) {
    buffer.insert_value(numbers, false);
    index++;
  }
  buffer.reset();

  // buffer.insert_value('X');
  // buffer.insert_value('X');
  // buffer.insert_value('X');

  auto it = buffer.data();

  EXPECT_EQ(*it, '4');
  it++;
  EXPECT_EQ(*it, '5');
  // EXPECT_EQ(*(buffer.data() + 2u), '6');
}
}  // namespace opossum
