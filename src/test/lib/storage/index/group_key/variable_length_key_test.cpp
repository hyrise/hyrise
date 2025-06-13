#include "base_test.hpp"
#include "storage/index/group_key/variable_length_key.hpp"

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
 * Test that copy‐assignment correctly duplicates the bit pattern
 * (this also covers operator=(const VariableLengthKeyBase&)).
 */
TEST_F(VariableLengthKeyTest, CopyAssignment) {
  const auto bytes = CompositeKeyLength{8};

  auto original = VariableLengthKey{bytes};
  original <<= CompositeKeyLength{4};
  original |= uint64_t{0xFFFFFFFFu};

  auto copy = VariableLengthKey{bytes};
  EXPECT_NE(copy, original);
  copy = original;
  EXPECT_EQ(copy, original);
}

/**
 * Test operator!= between two distinct keys and that identical keys compare equal.
 */
TEST_F(VariableLengthKeyTest, InequalityOperator) {
  const auto bytes = CompositeKeyLength{4};

  auto a = VariableLengthKey{bytes};
  a.shift_and_set(uint64_t{1}, uint8_t{8});

  auto b = VariableLengthKey{bytes};
  b.shift_and_set(uint64_t{2}, uint8_t{8});

  EXPECT_TRUE(a != b);
  EXPECT_FALSE(a != a);
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
  key.shift_and_set(uint64_t{0xABCD}, uint8_t{16});

  std::ostringstream oss;
  oss << key;
  const auto str = oss.str();
  EXPECT_FALSE(str.empty());
}


}  // namespace hyrise
