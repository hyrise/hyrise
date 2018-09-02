#include "join_hash_test.hpp"

namespace opossum {

template <typename T>
class JoinHashTypesTest : public JoinHashTest {};

using DataTypes = ::testing::Types<int, float, double>;
TYPED_TEST_CASE(JoinHashTypesTest, DataTypes);

TYPED_TEST(JoinHashTypesTest, BuildSingleValueLargePosList) {
  int test_item_count = 500;
  std::vector<TypeParam> values;
  for (int i = 0; i < test_item_count; ++i) {
  	values.push_back(static_cast<TypeParam>(17));
  }

  this->template test_hash_map<TypeParam, TypeParam>(values);
}

TYPED_TEST(JoinHashTypesTest, BuildSingleRowIds) {
  int test_item_count = 500;
  std::vector<TypeParam> values;
  for (int i = 0; i < test_item_count; ++i) {
  	values.push_back(static_cast<TypeParam>(pow(i, 3)));
  }

  this->template test_hash_map<TypeParam, TypeParam>(values);
}

}  // namespace opossum
