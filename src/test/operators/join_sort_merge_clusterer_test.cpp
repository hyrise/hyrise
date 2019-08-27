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
    _table_tpch_orders =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/tpch/sf-0.001/orders.tbl", 10));
    _table_tpch_orders->execute();

    _table_tpch_lineitems =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/tpch/sf-0.001/lineitem.tbl", 10));
    _table_tpch_lineitems->execute();

    _table_tpch_region =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/tpch/sf-0.001/region.tbl", 10));
    _table_tpch_region->execute();

    _table_with_nulls =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int4_with_null.tbl", 10));
    _table_with_nulls->execute();

    // filters retain all rows
    _table_tpch_orders_scanned = create_table_scan(_table_tpch_orders, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_orders_scanned->execute();
    _table_tpch_lineitems_scanned =
        create_table_scan(_table_tpch_lineitems, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_lineitems_scanned->execute();
  }

  void SetUp() override {
    const auto dummy_table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);
  }

  std::shared_ptr<AbstractOperator> dummy_input;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems,
      _table_with_nulls, _table_tpch_region;
  inline static std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;
};

TEST_F(JoinSortMergeClustererTest, RangeClustering) {
  constexpr auto cluster_count = size_t{4};
  // TODO: table wrapper to table?
  // Region and orders cover single- and multi-chunk inputs
  auto clusterer = JoinSortMergeClusterer<int>(_table_tpch_region->get_output(), _table_tpch_orders->get_output(), ColumnIDPair(ColumnID{0}, ColumnID{0}), false, false, false, cluster_count);

  auto output = clusterer.execute();

  auto left_row_count = size_t{0};
  auto right_row_count = size_t{0};
  auto left_largest_value_previous_cluster = std::numeric_limits<int>::min();
  auto right_largest_value_previous_cluster = std::numeric_limits<int>::min();

  EXPECT_EQ(output.clusters_left.size(), cluster_count);
  EXPECT_EQ(output.clusters_right.size(), cluster_count);
  for (auto cluster_id = size_t{0}; cluster_id < output.clusters_left.size(); ++cluster_id) {
    auto left_cluster = output.clusters_left[cluster_id];
    auto right_cluster = output.clusters_right[cluster_id];
    left_row_count += left_cluster->size();
    right_row_count += right_cluster->size();
    EXPECT_TRUE(std::is_sorted(left_cluster->begin(), left_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
    EXPECT_TRUE(std::is_sorted(right_cluster->begin(), right_cluster->end(), [](auto& left, auto& right) { return left.value < right.value; }));
    if (left_cluster->size() > 0) {
      EXPECT_LT(left_largest_value_previous_cluster, left_cluster->at(0).value);
      left_largest_value_previous_cluster = left_cluster->back().value;
    }
    if (right_cluster->size() > 0) {
      EXPECT_LT(right_largest_value_previous_cluster, right_cluster->at(0).value);
      right_largest_value_previous_cluster = right_cluster->back().value;
    }
  }

  EXPECT_EQ(_table_tpch_region->get_output()->row_count(), left_row_count);
  EXPECT_EQ(_table_tpch_orders->get_output()->row_count(), right_row_count);
}

TEST_F(JoinSortMergeClustererTest, RadixClusteringWithUnexpectedClusterCount) {
  // Radix clustering should not work with clusters counts that are not a power of two
  if (!HYRISE_DEBUG) GTEST_SKIP();

  constexpr auto cluster_count = size_t{17};
  auto clusterer = JoinSortMergeClusterer<int>(_table_tpch_region->get_output(), _table_tpch_region->get_output(), ColumnIDPair(ColumnID{0}, ColumnID{0}), true, false, false, cluster_count);
  EXPECT_THROW(clusterer.execute(), std::logic_error);
}

TEST_F(JoinSortMergeClustererTest, RadixClustering) {
  constexpr auto cluster_count = size_t{4};

  // Region and orders cover single- and multi-chunk inputs
  auto clusterer = JoinSortMergeClusterer<int>(_table_tpch_region->get_output(), _table_tpch_orders->get_output(), ColumnIDPair(ColumnID{0}, ColumnID{0}), true, false, false, cluster_count);
  auto output = clusterer.execute();

  auto left_row_count = size_t{0};
  auto right_row_count = size_t{0};

  EXPECT_EQ(output.clusters_left.size(), cluster_count);
  EXPECT_EQ(output.clusters_right.size(), cluster_count);
  for (auto cluster_id = size_t{0}; cluster_id < output.clusters_left.size(); ++cluster_id) {
    auto left_cluster = output.clusters_left[cluster_id];
    auto right_cluster = output.clusters_right[cluster_id];
    left_row_count += left_cluster->size();
    right_row_count += right_cluster->size();
  }

  EXPECT_EQ(_table_tpch_region->get_output()->row_count(), left_row_count);
  EXPECT_EQ(_table_tpch_orders->get_output()->row_count(), right_row_count);
}

// TEST_F(JoinSortMergeClustererTest, RadixClustering) {
  // constexpr auto cluster_count = size_t{4};

  // auto clusterer = JoinSortMergeClusterer<int>(_table_tpch_orders->get_output(), _table_tpch_orders->get_output(), ColumnIDPair(ColumnID{0}, ColumnID{0}), true, false, false, cluster_count);
  // auto output = clusterer.execute();
  // auto right_clusters = output.clusters_right;

  // auto row_count = size_t{0};
  // EXPECT_EQ(right_clusters.size(), cluster_count);
  // for (const auto& materialized_segment: right_clusters) {
  //   row_count += materialized_segment->size();
  // }
  // EXPECT_EQ(_table_tpch_orders->get_output()->row_count(), row_count);
// }

TEST_F(JoinSortMergeClustererTest, RadixClusteringSingleValue) {
  constexpr auto cluster_count = size_t{4};
  constexpr auto rows_to_create = size_t{20};

  auto column_definitions = TableColumnDefinitions{{"column_1", DataType::Int}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 100);
  for (auto row = size_t{0}; row < rows_to_create; ++row) {
    table->append({17});
  }

  // TODO: check that both sides have the same container

  auto clusterer = JoinSortMergeClusterer<int>(table, table, ColumnIDPair(ColumnID{0}, ColumnID{0}), true, false, false, cluster_count);
  auto output = clusterer.execute();
  auto right_clusters = output.clusters_right;

  auto row_count = size_t{0};
  EXPECT_EQ(right_clusters.size(), cluster_count);
  for (const auto& materialized_segment: right_clusters) {
    const auto segment_row_count = materialized_segment->size();
    row_count += segment_row_count;

    // As all values of the generated table have the same value, they should be put to the same cluster
    EXPECT_TRUE(segment_row_count == 0 || segment_row_count == rows_to_create);
  }
  EXPECT_EQ(_table_tpch_orders->get_output()->row_count(), row_count);
}

TEST_F(JoinSortMergeClustererTest, ConcatenateForSingleCluster) { EXPECT_TRUE(false); }

TEST_F(JoinSortMergeClustererTest, SortPartiallySortedSegments) {
  // empty or small list shall not throw even for empty sorted run position vectors
  auto segment = MaterializedSegment<int>{};

  auto empty_positions_1 = std::make_unique<std::vector<size_t>>();
  if (HYRISE_DEBUG) EXPECT_NO_THROW(JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(empty_positions_1)));
  segment.push_back({RowID{ChunkID{9u}, ChunkOffset{9u}}, 1});
  auto empty_positions_2 = std::make_unique<std::vector<size_t>>();
  if (HYRISE_DEBUG) EXPECT_NO_THROW(JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(empty_positions_2)));

  // for all segments with more elements, the vector of sorted run positions cannot be empty
  segment.push_back({RowID{ChunkID{8u}, ChunkOffset{8u}}, 2});
  segment.push_back({RowID{ChunkID{7u}, ChunkOffset{7u}}, 3});
  auto empty_positions_3 = std::make_unique<std::vector<size_t>>();
  if (HYRISE_DEBUG) EXPECT_THROW(JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(empty_positions_3)), std::logic_error);

  segment.push_back({RowID{ChunkID{6u}, ChunkOffset{6u}}, 1});
  segment.push_back({RowID{ChunkID{5u}, ChunkOffset{5u}}, 2});
  segment.push_back({RowID{ChunkID{4u}, ChunkOffset{4u}}, 3});
  segment.push_back({RowID{ChunkID{3u}, ChunkOffset{3u}}, 1});
  segment.push_back({RowID{ChunkID{2u}, ChunkOffset{2u}}, 2});
  segment.push_back({RowID{ChunkID{1u}, ChunkOffset{1u}}, 3});
  EXPECT_EQ(segment.size(), 9ul);  // ensure test data is correct
  auto positions = std::make_unique<std::vector<size_t>>(std::initializer_list<size_t>({0ul, 3ul, 6ul}));
  JoinSortMergeClusterer<int>::merge_partially_sorted_materialized_segment(segment, std::move(positions));

  EXPECT_TRUE(std::is_sorted(segment.begin(), segment.end(),
                             [](auto& left, auto& right) { return left.value < right.value; }));
}

}  // namespace opossum
