#include <string>

#include "base_test.hpp"

#include "operators/join_hash/join_hash_traits.hpp"

namespace opossum {

class JoinHashTraitTest : public BaseTest {};

#define EXPECT_HASH_TYPE(left, right, hash) EXPECT_TRUE((std::is_same_v<hash, JoinHashTraits<left, right>::HashType>))

TEST_F(JoinHashTraitTest, IntegerTraits) {
  // joining int and int
  EXPECT_HASH_TYPE(int32_t, int32_t, int32_t);

  // joining long and long
  EXPECT_HASH_TYPE(int64_t, int64_t, int64_t);

  // joining int and long
  EXPECT_HASH_TYPE(int32_t, int64_t, int64_t);
  EXPECT_HASH_TYPE(int64_t, int32_t, int64_t);
}

TEST_F(JoinHashTraitTest, FloatingTraits) {
  // joining float and float
  EXPECT_HASH_TYPE(float, float, float);

  // joining double and double
  EXPECT_HASH_TYPE(double, double, double);

  // joining float and double
  EXPECT_HASH_TYPE(float, double, double);
  EXPECT_HASH_TYPE(double, float, double);
}

TEST_F(JoinHashTraitTest, StringTraits) {
  // joining string and string
  EXPECT_HASH_TYPE(pmr_string, pmr_string, pmr_string);
}

TEST_F(JoinHashTraitTest, MixedNumberTraits) {
  // joining int and float
  EXPECT_HASH_TYPE(int32_t, float, float);
  EXPECT_HASH_TYPE(float, int32_t, float);

  // joining int and double
  EXPECT_HASH_TYPE(int32_t, double, double);
  EXPECT_HASH_TYPE(double, int32_t, double);

  // joining long and float
  EXPECT_HASH_TYPE(int64_t, float, float);
  EXPECT_HASH_TYPE(float, int64_t, float);

  // joining long and double
  EXPECT_HASH_TYPE(int64_t, double, double);
  EXPECT_HASH_TYPE(double, int64_t, double);
}

TEST_F(JoinHashTraitTest, MixedStringTraits) {
  // joining string and int
  EXPECT_HASH_TYPE(pmr_string, int32_t, pmr_string);
  EXPECT_HASH_TYPE(int32_t, pmr_string, pmr_string);

  // joining string and long
  EXPECT_HASH_TYPE(pmr_string, int64_t, pmr_string);
  EXPECT_HASH_TYPE(int64_t, pmr_string, pmr_string);

  // joining string and float
  EXPECT_HASH_TYPE(pmr_string, float, pmr_string);
  EXPECT_HASH_TYPE(float, pmr_string, pmr_string);

  // joining string and double
  EXPECT_HASH_TYPE(pmr_string, double, pmr_string);
  EXPECT_HASH_TYPE(double, pmr_string, pmr_string);
}

}  // namespace opossum
