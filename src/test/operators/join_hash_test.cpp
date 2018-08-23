#include <type_traits>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/join_hash.hpp"
#include "operators/join_hash/hash_traits.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for the JoinHash implementation.
*/

class JoinHashTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper_small = std::make_shared<TableWrapper>(load_table("src/test/tables/joinoperators/anti_int4.tbl", 2));
    _table_wrapper_small->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_small;
};

#define EXPECT_HASH_TYPE(left, right, hash) EXPECT_TRUE((std::is_same_v<hash, JoinHashTraits<left, right>::HashType>))
#define EXPECT_LEXICAL_CAST(left, right, cast) EXPECT_EQ((JoinHashTraits<left, right>::needs_lexical_cast), (cast))

TEST_F(JoinHashTest, IntegerTraits) {
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

TEST_F(JoinHashTest, FloatingTraits) {
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

TEST_F(JoinHashTest, StringTraits) {
  // joining string and string
  EXPECT_HASH_TYPE(std::string, std::string, std::string);
  EXPECT_LEXICAL_CAST(std::string, std::string, true);
}

TEST_F(JoinHashTest, MixedNumberTraits) {
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

TEST_F(JoinHashTest, MixedStringTraits) {
  // joining string and int
  EXPECT_HASH_TYPE(std::string, int32_t, std::string);
  EXPECT_HASH_TYPE(int32_t, std::string, std::string);
  EXPECT_LEXICAL_CAST(std::string, int32_t, true);
  EXPECT_LEXICAL_CAST(int32_t, std::string, true);

  // joining string and long
  EXPECT_HASH_TYPE(std::string, int64_t, std::string);
  EXPECT_HASH_TYPE(int64_t, std::string, std::string);
  EXPECT_LEXICAL_CAST(std::string, int64_t, true);
  EXPECT_LEXICAL_CAST(int64_t, std::string, true);

  // joining string and float
  EXPECT_HASH_TYPE(std::string, float, std::string);
  EXPECT_HASH_TYPE(float, std::string, std::string);
  EXPECT_LEXICAL_CAST(std::string, float, true);
  EXPECT_LEXICAL_CAST(float, std::string, true);

  // joining string and double
  EXPECT_HASH_TYPE(std::string, double, std::string);
  EXPECT_HASH_TYPE(double, std::string, std::string);
  EXPECT_LEXICAL_CAST(std::string, double, true);
  EXPECT_LEXICAL_CAST(double, std::string, true);
}

TEST_F(JoinHashTest, OperatorName) {
  auto join = std::make_shared<JoinHash>(_table_wrapper_small, _table_wrapper_small, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);

  EXPECT_EQ(join->name(), "JoinHash");
}

}  // namespace opossum
