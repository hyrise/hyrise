#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/join_hash/join_hash_traits.hpp"

namespace opossum {

class JoinHashTraitTest : public BaseTest {};

#define EXPECT_HASH_TYPE(left, right, hash) EXPECT_TRUE((std::is_same_v<hash, JoinHashTraits<left, right>::HashType>))
#define EXPECT_LEXICAL_CAST(left, right, cast) EXPECT_EQ((JoinHashTraits<left, right>::needs_lexical_cast), (cast))

TEST_F(JoinHashTraitTest, IntegerTraits) {
  // joining int and int
  EXPECT_HASH_TYPE(int32_t, int32_t, int32_t);
  EXPECT_LEXICAL_CAST(int32_t, int32_t, false);

  // joining long and long
  EXPECT_HASH_TYPE(int64_t, int64_t, int64_t);
  EXPECT_LEXICAL_CAST(int64_t, int64_t, false);

  // joining int and long
  EXPECT_HASH_TYPE(int32_t, int64_t, int64_t);
  EXPECT_HASH_TYPE(int64_t, int32_t, int64_t);
  EXPECT_LEXICAL_CAST(int32_t, int64_t, false);
  EXPECT_LEXICAL_CAST(int64_t, int32_t, false);
}

TEST_F(JoinHashTraitTest, FloatingTraits) {
  // joining float and float
  EXPECT_HASH_TYPE(float, float, float);
  EXPECT_LEXICAL_CAST(float, float, false);

  // joining double and double
  EXPECT_HASH_TYPE(double, double, double);
  EXPECT_LEXICAL_CAST(double, double, false);

  // joining float and double
  EXPECT_HASH_TYPE(float, double, double);
  EXPECT_HASH_TYPE(double, float, double);
  EXPECT_LEXICAL_CAST(float, double, false);
  EXPECT_LEXICAL_CAST(double, float, false);
}

TEST_F(JoinHashTraitTest, StringTraits) {
  // joining string and string
  EXPECT_HASH_TYPE(pmr_string, pmr_string, pmr_string);
  EXPECT_LEXICAL_CAST(pmr_string, pmr_string, true);
}

TEST_F(JoinHashTraitTest, MixedNumberTraits) {
  // joining int and float
  EXPECT_HASH_TYPE(int32_t, float, float);
  EXPECT_HASH_TYPE(float, int32_t, float);
  EXPECT_LEXICAL_CAST(int32_t, float, false);
  EXPECT_LEXICAL_CAST(float, int32_t, false);

  // joining int and double
  EXPECT_HASH_TYPE(int32_t, double, double);
  EXPECT_HASH_TYPE(double, int32_t, double);
  EXPECT_LEXICAL_CAST(int32_t, double, false);
  EXPECT_LEXICAL_CAST(double, int32_t, false);

  // joining long and float
  EXPECT_HASH_TYPE(int64_t, float, float);
  EXPECT_HASH_TYPE(float, int64_t, float);
  EXPECT_LEXICAL_CAST(int64_t, float, false);
  EXPECT_LEXICAL_CAST(float, int64_t, false);

  // joining long and double
  EXPECT_HASH_TYPE(int64_t, double, double);
  EXPECT_HASH_TYPE(double, int64_t, double);
  EXPECT_LEXICAL_CAST(int64_t, double, false);
  EXPECT_LEXICAL_CAST(double, int64_t, false);
}

TEST_F(JoinHashTraitTest, MixedStringTraits) {
  // joining string and int
  EXPECT_HASH_TYPE(pmr_string, int32_t, pmr_string);
  EXPECT_HASH_TYPE(int32_t, pmr_string, pmr_string);
  EXPECT_LEXICAL_CAST(pmr_string, int32_t, true);
  EXPECT_LEXICAL_CAST(int32_t, pmr_string, true);

  // joining string and long
  EXPECT_HASH_TYPE(pmr_string, int64_t, pmr_string);
  EXPECT_HASH_TYPE(int64_t, pmr_string, pmr_string);
  EXPECT_LEXICAL_CAST(pmr_string, int64_t, true);
  EXPECT_LEXICAL_CAST(int64_t, pmr_string, true);

  // joining string and float
  EXPECT_HASH_TYPE(pmr_string, float, pmr_string);
  EXPECT_HASH_TYPE(float, pmr_string, pmr_string);
  EXPECT_LEXICAL_CAST(pmr_string, float, true);
  EXPECT_LEXICAL_CAST(float, pmr_string, true);

  // joining string and double
  EXPECT_HASH_TYPE(pmr_string, double, pmr_string);
  EXPECT_HASH_TYPE(double, pmr_string, pmr_string);
  EXPECT_LEXICAL_CAST(pmr_string, double, true);
  EXPECT_LEXICAL_CAST(double, pmr_string, true);
}

}  // namespace opossum
