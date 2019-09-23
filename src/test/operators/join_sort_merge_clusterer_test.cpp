#include <limits>

#include "../base_test.hpp"

#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/join_sort_merge/column_materializer.hpp"
#include "operators/join_sort_merge/join_sort_merge_clusterer.hpp"
#include "types.hpp"

namespace opossum {

class JoinSortMergeClustererTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_tpch_orders = load_table("resources/test_data/tbl/tpch/sf-0.001/orders.tbl", 10);
    _table_tpch_lineitems = load_table("resources/test_data/tbl/tpch/sf-0.001/lineitem.tbl", 10);
    _table_tpch_region = load_table("resources/test_data/tbl/tpch/sf-0.001/region.tbl", 10);
    _table_with_nulls = load_table("resources/test_data/tbl/int_int4_with_null.tbl", 10);
  }

  inline static std::shared_ptr<Table> _table_tpch_orders, _table_tpch_lineitems,
      _table_with_nulls, _table_tpch_region;
};

TEST_F(JoinSortMergeClustererTest, RangeClustering) {
  constexpr auto cluster_count = size_t{4};

  // Region and orders cover single- and multi-chunk inputs
  auto clusterer = JoinSortMergeClusterer<int>(_table_tpch_region, _table_tpch_orders, ColumnIDPair(ColumnID{0}, ColumnID{0}), false, false, false, cluster_count);
  auto output = clusterer.execute();

  auto left_row_count = size_t{0};
  auto right_row_count = size_t{0};
  auto left_largest_value_previous_cluster = std::numeric_limits<int>::min();
  auto right_largest_value_previous_cluster = std::numeric_limits<int>::min();

  ASSERT_EQ(output.clusters_left.size(), cluster_count);
  ASSERT_EQ(output.clusters_right.size(), cluster_count);
  for (auto cluster_id = size_t{0}; cluster_id < output.clusters_left.size(); ++cluster_id) {
    auto left_cluster = output.clusters_left[cluster_id];
    auto right_cluster = output.clusters_right[cluster_id];
    left_row_count += left_cluster->size();
    right_row_count += right_cluster->size();
    ASSERT_TRUE(std::is_sorted(left_cluster->begin(), left_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
    ASSERT_TRUE(std::is_sorted(right_cluster->begin(), right_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
    if (left_cluster->size() > 0) {
      ASSERT_LT(left_largest_value_previous_cluster, left_cluster->at(0).value);
      left_largest_value_previous_cluster = left_cluster->back().value;
    }
    if (right_cluster->size() > 0) {
      ASSERT_LT(right_largest_value_previous_cluster, right_cluster->at(0).value);
      right_largest_value_previous_cluster = right_cluster->back().value;
    }
  }

  ASSERT_EQ(_table_tpch_region->row_count(), left_row_count);
  ASSERT_EQ(_table_tpch_orders->row_count(), right_row_count);
}

TEST_F(JoinSortMergeClustererTest, RadixClusteringWithUnexpectedClusterCount) {
  // Radix clustering should not work with clusters counts that are not a power of two
  if (!HYRISE_DEBUG) GTEST_SKIP();

  constexpr auto cluster_count = size_t{17};
  ASSERT_THROW(JoinSortMergeClusterer<int>(_table_tpch_region, _table_tpch_region, ColumnIDPair(ColumnID{0}, ColumnID{0}), true, false, false, cluster_count), std::logic_error);
}

TEST_F(JoinSortMergeClustererTest, RadixClustering) {
  constexpr auto cluster_count = size_t{4};

  // Region and orders cover single- and multi-chunk inputs
  auto clusterer = JoinSortMergeClusterer<int>(_table_tpch_region, _table_tpch_orders, ColumnIDPair(ColumnID{0}, ColumnID{0}), true, false, false, cluster_count);
  auto output = clusterer.execute();

  auto left_row_count = size_t{0};
  auto right_row_count = size_t{0};

  ASSERT_EQ(output.clusters_left.size(), cluster_count);
  ASSERT_EQ(output.clusters_right.size(), cluster_count);
  for (auto cluster_id = size_t{0}; cluster_id < output.clusters_left.size(); ++cluster_id) {
    auto left_cluster = output.clusters_left[cluster_id];
    auto right_cluster = output.clusters_right[cluster_id];
    left_row_count += left_cluster->size();
    right_row_count += right_cluster->size();

    ASSERT_TRUE(std::is_sorted(left_cluster->begin(), left_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
    ASSERT_TRUE(std::is_sorted(right_cluster->begin(), right_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
  }

  ASSERT_EQ(_table_tpch_region->row_count(), left_row_count);
  ASSERT_EQ(_table_tpch_orders->row_count(), right_row_count);
}

TEST_F(JoinSortMergeClustererTest, MaterializeNullValues) {
  constexpr auto cluster_count = size_t{4};

  // Region and orders cover single- and multi-chunk inputs
  auto clusterer = JoinSortMergeClusterer<int>(_table_with_nulls, _table_with_nulls, ColumnIDPair(ColumnID{0}, ColumnID{1}), true, true, true, cluster_count);
  auto output = clusterer.execute();

  auto left_row_count = size_t{0};
  auto right_row_count = size_t{0};

  ASSERT_EQ(output.clusters_left.size(), cluster_count);
  ASSERT_EQ(output.clusters_right.size(), cluster_count);
  for (auto cluster_id = size_t{0}; cluster_id < output.clusters_left.size(); ++cluster_id) {
    auto left_cluster = output.clusters_left[cluster_id];
    auto right_cluster = output.clusters_right[cluster_id];
    left_row_count += left_cluster->size();
    right_row_count += right_cluster->size();

    ASSERT_TRUE(std::is_sorted(left_cluster->begin(), left_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
    ASSERT_TRUE(std::is_sorted(right_cluster->begin(), right_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
  }

  ASSERT_EQ(_table_with_nulls->row_count() - 2, left_row_count);  // two NULLs
  ASSERT_EQ(_table_with_nulls->row_count() - 3, right_row_count);  // three NULLs
  ASSERT_EQ(output.null_rows_left->size(), 2ul);
  ASSERT_EQ(output.null_rows_right->size(), 3ul);
}

TEST_F(JoinSortMergeClustererTest, DoNotMaterializeNullValues) {
  constexpr auto cluster_count = size_t{4};

  // Region and orders cover single- and multi-chunk inputs
  auto clusterer = JoinSortMergeClusterer<int>(_table_with_nulls, _table_with_nulls, ColumnIDPair(ColumnID{0}, ColumnID{1}), true, false, false, cluster_count);
  auto output = clusterer.execute();

  auto left_row_count = size_t{0};
  auto right_row_count = size_t{0};

  ASSERT_EQ(output.clusters_left.size(), cluster_count);
  ASSERT_EQ(output.clusters_right.size(), cluster_count);
  for (auto cluster_id = size_t{0}; cluster_id < output.clusters_left.size(); ++cluster_id) {
    auto left_cluster = output.clusters_left[cluster_id];
    auto right_cluster = output.clusters_right[cluster_id];
    left_row_count += left_cluster->size();
    right_row_count += right_cluster->size();

    ASSERT_TRUE(std::is_sorted(left_cluster->begin(), left_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
    ASSERT_TRUE(std::is_sorted(right_cluster->begin(), right_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
  }

  ASSERT_EQ(_table_with_nulls->row_count() - 2, left_row_count);  // two NULLs
  ASSERT_EQ(_table_with_nulls->row_count() - 3, right_row_count);  // three NULLs
  ASSERT_TRUE(output.null_rows_left->empty());
  ASSERT_TRUE(output.null_rows_right->empty());
}

TEST_F(JoinSortMergeClustererTest, RadixClusteringSingleValue) {
  constexpr auto cluster_count = size_t{4};
  constexpr auto rows_to_create = size_t{20};

  auto column_definitions = TableColumnDefinitions{{"column_1", DataType::Int, false}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 100);
  for (auto row = size_t{0}; row < rows_to_create; ++row) {
    table->append({17});
  }

  auto clusterer = JoinSortMergeClusterer<int>(table, table, ColumnIDPair(ColumnID{0}, ColumnID{0}), true, false, false, cluster_count);
  auto output = clusterer.execute();
  
  auto left_row_count = size_t{0};
  auto right_row_count = size_t{0};

  ASSERT_EQ(output.clusters_left.size(), cluster_count);
  ASSERT_EQ(output.clusters_right.size(), cluster_count);
  for (auto cluster_id = size_t{0}; cluster_id < output.clusters_left.size(); ++cluster_id) {
    auto left_cluster = output.clusters_left[cluster_id];
    auto right_cluster = output.clusters_right[cluster_id];
    const auto left_cluster_row_count = left_cluster->size();
    const auto right_cluster_row_count = right_cluster->size();
    left_row_count += left_cluster_row_count;
    right_row_count += right_cluster_row_count;

    // As all values of the generated table have the same value, they should be put to the same cluster
    ASSERT_TRUE(left_row_count == 0 || left_row_count == rows_to_create);
    ASSERT_TRUE(right_row_count == 0 || right_row_count == rows_to_create);
    ASSERT_TRUE(left_row_count == right_row_count);
  }

  ASSERT_EQ(rows_to_create, left_row_count);
  ASSERT_EQ(rows_to_create, right_row_count);
}

TEST_F(JoinSortMergeClustererTest, ConcatenateForSingleCluster) {
  constexpr auto cluster_count = size_t{1};

  auto clusterer = JoinSortMergeClusterer<int>(_table_tpch_region, _table_tpch_orders, ColumnIDPair(ColumnID{0}, ColumnID{1}), false, false, false, cluster_count);
  auto output = clusterer.execute();

  const auto left_single_cluster = output.clusters_left;
  const auto right_single_cluster = output.clusters_right;

  ASSERT_EQ(left_single_cluster.size(), 1);
  ASSERT_EQ(right_single_cluster.size(), 1);

  ASSERT_TRUE(std::is_sorted(left_single_cluster[0]->begin(), left_single_cluster[0]->end(),
                             [](auto& left, auto& right) { return left.value < right.value; }));
  ASSERT_TRUE(std::is_sorted(right_single_cluster[0]->begin(), right_single_cluster[0]->end(),
                             [](auto& left, auto& right) { return left.value < right.value; }));
}

TEST_F(JoinSortMergeClustererTest, PickSampleValues) {
  if (HYRISE_DEBUG) {
    ASSERT_THROW(JoinSortMergeClusterer<int>::pick_split_values({1, 2, 4, 5}, 1), std::logic_error);
  }

  {
    const auto result = JoinSortMergeClusterer<int>::pick_split_values({4, 5, 6, 7}, 4);
    ASSERT_EQ(result.size(), 3);
  }

  {
    const auto result = JoinSortMergeClusterer<int>::pick_split_values({1, 2, 4, 5}, 2);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), 4);
  }

  {
    const auto result = JoinSortMergeClusterer<int>::pick_split_values({1, 2, 3, 11, 12, 13, 21, 22, 23}, 3);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), 11);
    ASSERT_EQ(result[1], 21);
  }
}

TEST_F(JoinSortMergeClustererTest, RangeClusterFunctorSorted) {
  // Clusters need to be sorted for range clustering (done by ColumnMaterializer)
  auto segment_1 = std::make_shared<MaterializedSegment<int>>();
  segment_1->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 1});
  segment_1->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 8});
  segment_1->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 9});
  auto segment_2 = std::make_shared<MaterializedSegment<int>>();
  segment_2->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 4});
  segment_2->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 5});
  segment_2->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 6});
  auto segment_3 = std::make_shared<MaterializedSegment<int>>();
  segment_3->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 1});
  segment_3->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 1});
  segment_3->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 2});
  segment_3->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 3});
  segment_3->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 7});
  segment_3->push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 10});

  auto segment_list_left = MaterializedSegmentList<int>{segment_1, segment_2};
  auto segment_list_right = MaterializedSegmentList<int>{segment_2, segment_3};
  auto result = JoinSortMergeClusterer<int>::range_cluster(segment_list_left, segment_list_right, {3, 5}, 3, {true, true});

  // We expect the left being clustered (values) into (1, 2), (3, 4), (5, 6) and the right into (2), (3, 4), and (5, 6, 7)
  ASSERT_EQ(result.first.size(), 3);
  ASSERT_EQ(result.second.size(), 3);

  ASSERT_EQ(result.first[0]->size(), 1);
  ASSERT_EQ(result.second[0]->size(), 3);
  ASSERT_EQ(result.first[1]->size(), 1);
  ASSERT_EQ(result.second[1]->size(), 2);
  ASSERT_EQ(result.first[2]->size(), 4);
  ASSERT_EQ(result.second[2]->size(), 4);
}

TEST_F(JoinSortMergeClustererTest, SortPartiallySortedSegments) {
  // empty or small list shall not throw even for empty sorted run position vectors
  auto segment = MaterializedSegment<int>{};

  auto empty_positions_1 = std::make_unique<std::vector<size_t>>();
  if (HYRISE_DEBUG) { ASSERT_NO_THROW(JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(empty_positions_1))); }
  segment.push_back({RowID{ChunkID{9u}, ChunkOffset{9u}}, 1});
  auto empty_positions_2 = std::make_unique<std::vector<size_t>>();
  if (HYRISE_DEBUG) { ASSERT_NO_THROW(JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(empty_positions_2))); }

  // for all segments with more elements, the vector of sorted run positions cannot be empty
  segment.push_back({RowID{ChunkID{8u}, ChunkOffset{8u}}, 2});
  segment.push_back({RowID{ChunkID{7u}, ChunkOffset{7u}}, 3});
  auto empty_positions_3 = std::make_unique<std::vector<size_t>>();
  if (HYRISE_DEBUG) { ASSERT_THROW(JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(empty_positions_3)), std::logic_error); }

  segment.push_back({RowID{ChunkID{6u}, ChunkOffset{6u}}, 1});
  segment.push_back({RowID{ChunkID{5u}, ChunkOffset{5u}}, 2});
  segment.push_back({RowID{ChunkID{4u}, ChunkOffset{4u}}, 3});
  segment.push_back({RowID{ChunkID{3u}, ChunkOffset{3u}}, 1});
  segment.push_back({RowID{ChunkID{2u}, ChunkOffset{2u}}, 2});
  segment.push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 3});
  ASSERT_EQ(segment.size(), 9ul);  // ensure test data is correct
  auto positions = std::make_unique<std::vector<size_t>>(std::initializer_list<size_t>({0ul, 3ul, 6ul}));
  JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(positions));

  ASSERT_TRUE(std::is_sorted(segment.begin(), segment.end(),
                             [](auto& left, auto& right) { return left.value < right.value; }));
}

// Test that sorted runs concisting of single values are handled properly.
TEST_F(JoinSortMergeClustererTest, SortPartiallySortedSingleValueSegments) {
  auto segment = MaterializedSegment<int>{};

  segment.push_back({RowID{ChunkID{6u}, ChunkOffset{6u}}, 1});
  segment.push_back({RowID{ChunkID{5u}, ChunkOffset{5u}}, 2});
  segment.push_back({RowID{ChunkID{4u}, ChunkOffset{4u}}, 3});
  segment.push_back({RowID{ChunkID{3u}, ChunkOffset{3u}}, 1});
  segment.push_back({RowID{ChunkID{2u}, ChunkOffset{2u}}, 2});
  segment.push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 3});
  auto positions = std::make_unique<std::vector<size_t>>(std::initializer_list<size_t>({0ul, 1ul, 2ul, 3ul}));
  JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(positions));

  ASSERT_TRUE(std::is_sorted(segment.begin(), segment.end(),
                             [](auto& left, auto& right) { return left.value < right.value; }));
}

}  // namespace opossum
