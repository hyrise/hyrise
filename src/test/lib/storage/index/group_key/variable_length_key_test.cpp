#include <gtest/gtest.h>

#include <cstdint>

#include "base_test.hpp"
#include "storage/index/group_key/variable_length_key.hpp"
#include "storage/index/group_key/variable_length_key_base.hpp"

namespace hyrise {

class VariableLengthKeyTest : public BaseTest {
 protected:
  void SetUp() override {
    _key_reference = VariableLengthKey(sizeof(uint64_t));
    _key_reference |= 65793u;
  }

 protected:
  VariableLengthKey _key_reference;
};

TEST_F(VariableLengthKeyTest, CopyTest) {
  auto key = VariableLengthKey(sizeof(uint64_t));
  key |= 65793u;

  EXPECT_EQ(_key_reference, key);

  {
    auto copied_key = key;
    EXPECT_EQ(_key_reference, copied_key);
  }

  EXPECT_EQ(_key_reference, key);
}

TEST_F(VariableLengthKeyTest, MoveTest) {
  auto key = VariableLengthKey(sizeof(uint64_t));
  key |= 65793u;

  EXPECT_EQ(_key_reference, key);

  auto moved_key = key;
  EXPECT_EQ(_key_reference, moved_key);
}

TEST_F(VariableLengthKeyTest, CreateKeysWithOrAndShift) {
  auto key = VariableLengthKey(sizeof(uint64_t));
  key |= 1u;
  key <<= 8;
  key |= 1u;
  key <<= 8;
  key |= 1u;

  EXPECT_TRUE(key == _key_reference);
}

/**
 * Test that copy‐assignment correctly duplicates the bit pattern.
 */
TEST_F(VariableLengthKeyTest, CopyAssignment) {
  const auto bytes = CompositeKeyLength{8};

  auto original = VariableLengthKey{bytes};
  original.shift_and_set(1, 8);

  auto copy = VariableLengthKey{bytes};

  EXPECT_NE(copy, original);

  copy = original;
  EXPECT_EQ(copy, original);
}

/**
 * Test the plain <<= and |= operators.
 */
TEST_F(VariableLengthKeyTest, BasicShiftAndSet) {
  const auto bytes = CompositeKeyLength{8};

  auto original = VariableLengthKey{bytes};
  original |= uint64_t{0xABCDu};
  EXPECT_NE(original, VariableLengthKey{bytes});

  original <<= CompositeKeyLength{8};
  auto reference = VariableLengthKey{bytes};
  reference |= uint64_t{0xABCD00u};
  EXPECT_EQ(original, reference);
}

/**
 * Test operator!=, < and == between two distinct keys.
 */
TEST_F(VariableLengthKeyTest, ComparisonOperators) {
  const auto bytes = CompositeKeyLength{4};

  auto a = VariableLengthKey{bytes};
  auto b = VariableLengthKey{bytes};

  EXPECT_TRUE(a == b);

  a.shift_and_set(1, 8);
  b.shift_and_set(2, 8);

  EXPECT_TRUE(a != b);
  EXPECT_FALSE(a != a);
  EXPECT_TRUE(a < b);
  EXPECT_FALSE(b < a);
}

/**
 * Test bytes_per_key() returns the configured length.
 */
TEST_F(VariableLengthKeyTest, BytesPerKey) {
  const auto bytes = CompositeKeyLength{16};
  const auto key = VariableLengthKey{bytes};
  EXPECT_EQ(key.bytes_per_key(), bytes);
}

/**
 * Test that streaming a key to std::ostream produces a non‐empty string.
 */
TEST_F(VariableLengthKeyTest, OStreamOperator) {
  const auto bytes = CompositeKeyLength{2};
  auto key = VariableLengthKey{bytes};
  key.shift_and_set(0xABCD, 16);

  auto oss = std::ostringstream{};
  oss << key;
  const auto str = oss.str();
  EXPECT_EQ(str, "ab cd");
}

}  // namespace hyrise
