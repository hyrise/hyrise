#include "base_test.hpp"

#include "operators/join_hash/join_hash_steps.hpp"

namespace opossum {

/**
 * The purpose of these tests it to check the construction of hash maps which are not mapping value
 * to value but value to vector of RowIDs (i.e., PosList). We have two tests with sparse data
 * (no value occurs twice) and dense data (all rows share the same value).
 * All these tests are executed for the main numeric types.
 */
template <typename T>
class JoinHashTypesTest : public BaseTest {};

template <typename T, typename HashType>
void test_hash_map(const std::vector<T>& values) {
  Partition<T> partition;
  for (ChunkOffset i = ChunkOffset{0}; i < values.size(); ++i) {
    RowID row_id{ChunkID{17}, i};
    partition.elements.emplace_back(PartitionedElement<T>{row_id, static_cast<T>(values.at(i))});
    partition.null_values.emplace_back(false);
  }

  // Build a BloomFilter that cannot be used to skip any entries by creating a BloomFilter with every value being false
  // and using bitwise negation (~x).
  auto bloom_filter = ~BloomFilter(BLOOM_FILTER_SIZE);

  auto hash_maps = build<T, HashType>(RadixContainer<T>{partition}, JoinHashBuildMode::AllPositions, 0, bloom_filter);

  // With only one offset value passed, one hash map will be created
  EXPECT_EQ(hash_maps.size(), 1);

  const auto& first_hash_map = hash_maps.at(0).value();

  ChunkOffset offset = ChunkOffset{0};
  for (const auto& element : partition.elements) {
    const auto probe_value = element.value;

    const auto [begin, end] = first_hash_map.find(probe_value);
    const auto result_list = RowIDPosList{begin, end};

    const RowID probe_row_id{ChunkID{17}, offset};
    EXPECT_TRUE(std::find(result_list.begin(), result_list.end(), probe_row_id) != result_list.end());
    ++offset;
  }
}

using JoinHashTypesTestDataTypes = ::testing::Types<int, float, double>;
TYPED_TEST_SUITE(JoinHashTypesTest, JoinHashTypesTestDataTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(JoinHashTypesTest, BuildSingleValueLargePosList) {
  int test_item_count = 500;
  std::vector<TypeParam> values;
  for (int i = 0; i < test_item_count; ++i) {
    values.push_back(static_cast<TypeParam>(17));
  }

  test_hash_map<TypeParam, TypeParam>(values);
}

TYPED_TEST(JoinHashTypesTest, BuildSingleRowIds) {
  int test_item_count = 500;
  std::vector<TypeParam> values;
  for (int i = 0; i < test_item_count; ++i) {
    values.push_back(static_cast<TypeParam>(pow(i, 3)));
  }

  test_hash_map<TypeParam, TypeParam>(values);
}

}  // namespace opossum
