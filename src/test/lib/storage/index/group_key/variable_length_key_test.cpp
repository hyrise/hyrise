#include "base_test.hpp"

#include "storage/index/group_key/variable_length_key.hpp"

namespace opossum {

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

}  // namespace opossum
