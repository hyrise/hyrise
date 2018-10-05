#include "../base_test.hpp"
#include "gtest/gtest.h"

// #include "operators/join_hash.cpp"  // to access free functions build() etc.
#include "operators/join_hash.hpp"
#include "operators/join_hash/hash_functions.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

#include <chrono>

namespace opossum {

/*
This contains the tests for the JoinHash implementation.
*/

class JoinHashTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper_small = std::make_shared<TableWrapper>(load_table("src/test/tables/joinoperators/anti_int4.tbl", 2));
    _table_wrapper_small->execute();

    _table_tpch_orders = std::make_shared<TableWrapper>(load_table("src/test/tables/tpch/sf-0.001/orders.tbl", 10));
    _table_tpch_orders->execute();

    _table_tpch_lineitems =
        std::make_shared<TableWrapper>(load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl", 10));
    _table_tpch_lineitems->execute();

    _table_with_nulls = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int4_with_null.tbl", 10));
    _table_with_nulls->execute();

    // filters retains all rows
    _table_tpch_orders_scanned =
        std::make_shared<TableScan>(_table_tpch_orders, OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThan, 0});
    _table_tpch_orders_scanned->execute();
    _table_tpch_lineitems_scanned =
        std::make_shared<TableScan>(_table_tpch_lineitems, OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThan, 0});
    _table_tpch_lineitems_scanned->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems, _table_with_nulls;
  std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;

  // Accumulates the RowIDs hidden behind the iterator element (hash map stores PosLists, not RowIDs)
  template <typename Iter>
  size_t get_row_count(Iter begin, Iter end) {
    size_t row_count = 0;
    for (Iter it = begin; it != end; ++it) {
      row_count += it->second.size();
    }
    return row_count;
  }
};

TEST_F(JoinHashTest, OperatorName) {
  auto start = std::chrono::steady_clock::now();
  auto join = std::make_shared<JoinHash>(_table_wrapper_small, _table_wrapper_small, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
  auto finish = std::chrono::steady_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count();
  std::cout << "Took " << time << std::endl;

  EXPECT_EQ(join->name(), "JoinHash");
}

TEST_F(JoinHashTest, DISABLED_ChunkCount) {
  auto join = std::make_shared<JoinHash>(_table_tpch_orders_scanned, _table_tpch_lineitems_scanned, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals, 10);
  join->execute();

  // due to the per-chunk grouping in the outputting phase, the number of output chunks should
  // at max be the number of chunks in the input tables
  EXPECT_TRUE(join->get_output()->chunk_count() <=
              std::max(_table_tpch_orders_scanned->get_output()->chunk_count(),
                       _table_tpch_lineitems_scanned->get_output()->chunk_count()));
}

TEST_F(JoinHashTest, MaterializeInput) {
  std::vector<std::vector<size_t>> histograms;
  auto radix_container =
      materialize_input<int, int>(_table_tpch_lineitems_scanned->get_output(), ColumnID{0}, histograms, 0, 17);

  EXPECT_EQ(radix_container.elements->size(), _table_tpch_lineitems_scanned->get_output()->row_count());
}

TEST_F(JoinHashTest, MaterializeAndBuildWithKeepNulls) {
  size_t radix_bit_count = 0;
  std::vector<std::vector<size_t>> histograms;

  // we materialize the table twice, once with keeping NULL values and once without
  auto mat_with_nulls =
      materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, 17, true);
  auto mat_without_nulls =
      materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, 17, false);

  // Note: due to initialization with empty Partition Elements, NULL values are not materialized but
  // size of materialized input does not shrink due to NULL values.
  EXPECT_EQ(mat_with_nulls.elements->size(), mat_without_nulls.elements->size());
  // check if NULL values have been ignored
  EXPECT_EQ(mat_without_nulls.elements->at(6).value, 9);
  EXPECT_EQ(mat_with_nulls.elements->at(6).value, 13);

  // build phase: NULLs we be discarded
  auto hash_map_with_nulls = build<int, int>(mat_with_nulls);
  auto hash_map_without_nulls = build<int, int>(mat_without_nulls);

  // check for the expected number of hash maps
  EXPECT_EQ(hash_map_with_nulls.size(), pow(2, radix_bit_count));
  EXPECT_EQ(hash_map_without_nulls.size(), pow(2, radix_bit_count));

  // get count of non-NULL values in table
  auto table_without_nulls_scanned =
      std::make_shared<TableScan>(_table_with_nulls, OperatorScanPredicate{ColumnID{0}, PredicateCondition::IsNotNull, 0});
  table_without_nulls_scanned->execute();

  // now that build removed the unneeded init values, map sizes should differ
  EXPECT_EQ(this->get_row_count(hash_map_without_nulls.at(0).value().begin(), hash_map_without_nulls.at(0).value().end()),
    table_without_nulls_scanned->get_output()->row_count());
}

TEST_F(JoinHashTest, MaterializeInputHistograms) {
  std::vector<std::vector<size_t>> histograms;
  materialize_input<int, int>(_table_tpch_lineitems_scanned->get_output(), ColumnID{0}, histograms, 0, 17);

  size_t histogram_offset_sum = 0;
  for (const auto& radix_count_per_chunk : histograms) {
    for (auto count : radix_count_per_chunk) {
      histogram_offset_sum += count;
    }
  }

  EXPECT_EQ(histogram_offset_sum, _table_tpch_lineitems_scanned->get_output()->row_count());
}

}  // namespace opossum
