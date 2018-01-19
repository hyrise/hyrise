#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class HashFunctionTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(HashFunctionTest, HashInt32) {
  HashFunction hf;
  EXPECT_EQ(hf(42), hf(42));
  EXPECT_EQ(hf(21), hf(21));
  EXPECT_NE(hf(42), hf(21));
}

TEST_F(HashFunctionTest, HashInt64) {
  HashFunction hf;
  EXPECT_EQ(hf(42l), hf(42l));
  EXPECT_EQ(hf(21l), hf(21l));
  EXPECT_NE(hf(42l), hf(21l));
}

TEST_F(HashFunctionTest, HashFloat) {
  HashFunction hf;
  EXPECT_EQ(hf(4.2f), hf(4.2f));
  EXPECT_EQ(hf(2.1f), hf(2.1f));
  EXPECT_NE(hf(4.2f), hf(2.1f));
}

TEST_F(HashFunctionTest, HashDouble) {
  HashFunction hf;
  EXPECT_EQ(hf(4.2), hf(4.2));
  EXPECT_EQ(hf(2.1), hf(2.1));
  EXPECT_NE(hf(4.2), hf(2.1));
}

TEST_F(HashFunctionTest, HashString) {
  HashFunction hf;
  EXPECT_EQ(hf("42"), hf("42"));
  EXPECT_EQ(hf("21"), hf("21"));
  EXPECT_NE(hf("42"), hf("21"));
}

}  // namespace opossum
