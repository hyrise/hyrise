#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class HashFunctionTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(HashFunctionTest, HashInt32) {
  HashFunction hf;
  EXPECT_EQ(hf.calculate_hash(42), hf.calculate_hash(42));
  EXPECT_EQ(hf.calculate_hash(21), hf.calculate_hash(21));
  EXPECT_NE(hf.calculate_hash(42), hf.calculate_hash(21));
}

TEST_F(HashFunctionTest, HashInt64) {
  HashFunction hf;
  EXPECT_EQ(hf.calculate_hash(42l), hf.calculate_hash(42l));
  EXPECT_EQ(hf.calculate_hash(21l), hf.calculate_hash(21l));
  EXPECT_NE(hf.calculate_hash(42l), hf.calculate_hash(21l));
}

TEST_F(HashFunctionTest, HashFloat) {
  HashFunction hf;
  EXPECT_EQ(hf.calculate_hash(4.2f), hf.calculate_hash(4.2f));
  EXPECT_EQ(hf.calculate_hash(2.1f), hf.calculate_hash(2.1f));
  EXPECT_NE(hf.calculate_hash(4.2f), hf.calculate_hash(2.1f));
}

TEST_F(HashFunctionTest, HashDouble) {
  HashFunction hf;
  EXPECT_EQ(hf.calculate_hash(4.2), hf.calculate_hash(4.2));
  EXPECT_EQ(hf.calculate_hash(2.1), hf.calculate_hash(2.1));
  EXPECT_NE(hf.calculate_hash(4.2), hf.calculate_hash(2.1));
}

TEST_F(HashFunctionTest, HashString) {
  HashFunction hf;
  EXPECT_EQ(hf.calculate_hash("42"), hf.calculate_hash("42"));
  EXPECT_EQ(hf.calculate_hash("21"), hf.calculate_hash("21"));
  EXPECT_NE(hf.calculate_hash("42"), hf.calculate_hash("21"));
}

}  // namespace opossum
