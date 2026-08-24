#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "base_test.hpp"
#include "operators/aggregate/serialize.hpp"
#include "types.hpp"

namespace hyrise {
namespace {

class AggregateSerializeTest : public BaseTest {};

TEST_F(AggregateSerializeTest, SerializedSizeOfTriviallyCopyableValues) {
  EXPECT_EQ(serialized_value_size<int32_t>(false), sizeof(int32_t));
  EXPECT_EQ(serialized_value_size<int32_t>(true), sizeof(int32_t) + 1);
  EXPECT_EQ(serialized_value_size<double>(false), sizeof(double));
  EXPECT_EQ(serialized_value_size<double>(true), sizeof(double) + 1);
}

TEST_F(AggregateSerializeTest, SerializedSizeOfStringValues) {
  EXPECT_EQ(serialized_value_size<pmr_string>(false), ESTIMATED_AVERAGE_STRING_LENGTH);
  EXPECT_EQ(serialized_value_size<pmr_string>(true), ESTIMATED_AVERAGE_STRING_LENGTH + 1);
}

TEST_F(AggregateSerializeTest, SerializeTriviallyCopyableValues) {
  auto buffer = std::vector<std::byte>{std::byte{0x0a}};
  serialize_value(buffer, int8_t{0x0b});

  const auto expected = std::vector<std::byte>{std::byte{0x0a}, std::byte{0x0b}};
  EXPECT_EQ(buffer, expected);
}

TEST_F(AggregateSerializeTest, SerializeNullableTriviallyCopyableValues) {
  auto buffer = std::vector<std::byte>{std::byte{0x01}};
  serialize_value(buffer, int8_t{0x0b}, false);
  serialize_value(buffer, int8_t{}, true);

  const auto expected = std::vector<std::byte>{std::byte{0x01}, std::byte{0x00}, std::byte{0x0b}, std::byte{0x01}};
  EXPECT_EQ(buffer, expected);
}

TEST_F(AggregateSerializeTest, SerializeString) {
  auto buffer = std::vector<std::byte>{std::byte{'a'}};
  serialize_value(buffer, pmr_string{"bc"});

  const auto expected = std::vector<std::byte>{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  EXPECT_EQ(buffer, expected);
}

TEST_F(AggregateSerializeTest, SerializeNullableString) {
  auto buffer = std::vector<std::byte>{std::byte{0x01}};
  serialize_value(buffer, pmr_string("a"), false);
  serialize_value(buffer, pmr_string{}, true);

  const auto expected = std::vector<std::byte>{std::byte{0x01}, std::byte{0x00}, std::byte{'a'}, std::byte{0x01}};
  EXPECT_EQ(buffer, expected);
}

}  // namespace
}  // namespace hyrise
