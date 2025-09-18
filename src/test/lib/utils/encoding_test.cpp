#include <array>
#include <cstring>

#include "base_test.hpp"
#include "utils/encoding.hpp"

namespace hyrise {

class EncodingTest : public BaseTest {};

TEST_F(EncodingTest, EncodeIntegerLexicographicOrder) {
  auto buffer1 = std::array<std::byte, 5>{};
  auto buffer2 = std::array<std::byte, 5>{};  // 4 + 1 for MSB offset

  encode_integer<int32_t>(buffer1.data(), -1);
  encode_integer<int32_t>(buffer2.data(), 1);

  EXPECT_LT(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);

  encode_integer<int32_t>(buffer1.data(), 0);
  encode_integer<int32_t>(buffer2.data(), 1);
  EXPECT_LT(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);

  encode_integer<int32_t>(buffer1.data(), 1);
  encode_integer<int32_t>(buffer2.data(), 1);
  EXPECT_EQ(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);
}

TEST_F(EncodingTest, EncodeDoubleLexicographicOrder) {
  auto buffer1 = std::array<std::byte, 9>{};
  auto buffer2 = std::array<std::byte, 9>{};

  encode_double(buffer1.data(), -1.5);
  encode_double(buffer2.data(), 1.5);
  EXPECT_LT(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);

  encode_double(buffer1.data(), 0.0);
  encode_double(buffer2.data(), 1.5);
  EXPECT_LT(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);

  encode_double(buffer1.data(), 1.5);
  encode_double(buffer2.data(), 1.5);
  EXPECT_EQ(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);
}

TEST_F(EncodingTest, EncodeFloatLexicographicOrder) {
  auto buffer1 = std::array<std::byte, 5>{};
  auto buffer2 = std::array<std::byte, 5>{};

  encode_float(buffer1.data(), -123.45f);
  encode_float(buffer2.data(), 123.45f);
  EXPECT_LT(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);

  encode_float(buffer1.data(), 0.0f);
  encode_float(buffer2.data(), 123.45f);
  EXPECT_LT(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);

  encode_float(buffer1.data(), 123.45f);
  encode_float(buffer2.data(), 123.45f);
  EXPECT_EQ(std::memcmp(buffer1.data(), buffer2.data(), buffer1.size()), 0);
}

TEST_F(EncodingTest, EncodeStringCorrectLengthAndPadding) {
  auto buffer = std::array<std::byte, 10>{};
  auto value = pmr_string{"abc"};

  encode_string(buffer.data(), buffer.size(), value);

  EXPECT_EQ(static_cast<char>(buffer[0]), 'a');
  EXPECT_EQ(static_cast<char>(buffer[1]), 'b');
  EXPECT_EQ(static_cast<char>(buffer[2]), 'c');

  for (auto i = size_t{3}; i < 9; ++i) {
    EXPECT_EQ(buffer[i], std::byte{0});
  }

  EXPECT_EQ(static_cast<unsigned char>(buffer[9]), value.size());
}

TEST_F(EncodingTest, EncodeStringMaxLength) {
  const auto buffer_size = size_t{8};
  auto value = pmr_string{"abcdef"};  // 6 chars
  auto buffer = std::array<std::byte, buffer_size>{};

  encode_string(buffer.data(), buffer_size, value);

  EXPECT_EQ(static_cast<unsigned char>(buffer[buffer_size - 1]), value.size());
}

}  // namespace hyrise
