#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class StorageHashFunctionTest : public BaseTest {
 protected:
  void SetUp() override {}
};

/*
 * We want to make sure
 *  1. that hashing the same value *always* produces the same result
 *  2. that hashing different values *probably* produces different results
 * 
 * About 2 we can not be absolutely certains due to hash collisions. 
 * There are no hash collisions for the used values, though.
 * 
 * We tests this for all data types AllTypeVariant can hold.
 */

TEST_F(StorageHashFunctionTest, HashInt32) {
  HashFunction hf;
  EXPECT_EQ(hf(INT32_C(42)), hf(INT32_C(42)));
  EXPECT_EQ(hf(INT32_C(21)), hf(INT32_C(21)));
  EXPECT_NE(hf(INT32_C(42)), hf(INT32_C(21)));
}

TEST_F(StorageHashFunctionTest, HashInt64) {
  HashFunction hf;
  EXPECT_EQ(hf(INT64_C(42)), hf(INT64_C(42)));
  EXPECT_EQ(hf(INT64_C(21)), hf(INT64_C(21)));
  EXPECT_NE(hf(INT64_C(42)), hf(INT64_C(21)));
}

TEST_F(StorageHashFunctionTest, HashFloat) {
  HashFunction hf;
  EXPECT_EQ(hf(4.2f), hf(4.2f));
  EXPECT_EQ(hf(2.1f), hf(2.1f));
  EXPECT_NE(hf(4.2f), hf(2.1f));
}

TEST_F(StorageHashFunctionTest, HashDouble) {
  HashFunction hf;
  EXPECT_EQ(hf(4.2), hf(4.2));
  EXPECT_EQ(hf(2.1), hf(2.1));
  EXPECT_NE(hf(4.2), hf(2.1));
}

TEST_F(StorageHashFunctionTest, HashString) {
  HashFunction hf;
  EXPECT_EQ(hf("42"), hf("42"));
  EXPECT_EQ(hf("21"), hf("21"));
  EXPECT_NE(hf("42"), hf("21"));
}

TEST_F(StorageHashFunctionTest, HashNullValue) {
  HashFunction hf;
  NullValue nv;

  // NullValues are hashed to HashValue 0
  EXPECT_EQ(hf(nv), HashValue{0});
}

}  // namespace opossum
