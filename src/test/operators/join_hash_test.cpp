#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/join_hash.hpp"
#include "operators/join_hash/join_hash_steps.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for the JoinHash implementation.
*/

class JoinHashTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_size_zero_one = 1'000;

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    _table_zero_one = std::make_shared<Table>(column_definitions, TableType::Data, _table_size_zero_one);
    for (auto i = size_t{0}; i < _table_size_zero_one; ++i) {
      _table_zero_one->append({static_cast<int>(i % 2)});
    }

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
    _table_tpch_orders_scanned = create_table_scan(_table_tpch_orders, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_orders_scanned->execute();
    _table_tpch_lineitems_scanned =
        create_table_scan(_table_tpch_lineitems, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_lineitems_scanned->execute();
  }

  void SetUp() override {}

  static size_t _table_size_zero_one;
  static std::shared_ptr<Table> _table_zero_one;
  static std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems,
      _table_with_nulls;
  static std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;

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

size_t JoinHashTest::_table_size_zero_one = 0;
std::shared_ptr<Table> JoinHashTest::_table_zero_one = NULL;
std::shared_ptr<TableWrapper> JoinHashTest::_table_wrapper_small, JoinHashTest::_table_tpch_orders,
    JoinHashTest::_table_tpch_lineitems, JoinHashTest::_table_with_nulls = NULL;
std::shared_ptr<TableScan> JoinHashTest::_table_tpch_orders_scanned, JoinHashTest::_table_tpch_lineitems_scanned = NULL;

TEST_F(JoinHashTest, OperatorName) {
  auto join = std::make_shared<JoinHash>(_table_wrapper_small, _table_wrapper_small, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);

  EXPECT_EQ(join->name(), "JoinHash");
}

// Once we bring in the PosList optimization flag REFERS_TO_SINGLE_CHUNK_ONLY, this test will ensure
// that the join does not unnecessarily add chunks (e.g., discussed in #698).
TEST_F(JoinHashTest, DISABLED_ChunkCount /* #698 */) {
  auto join = std::make_shared<JoinHash>(_table_tpch_orders_scanned, _table_tpch_lineitems_scanned, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals, 10);
  join->execute();

  // While radix clustering is well-suited for very large tables, it also yield many output tables.
  // This test checks whether we create more chunks that existing in the input (which should not be the case).
  EXPECT_TRUE(join->get_output()->chunk_count() <=
              std::max(_table_tpch_orders_scanned->get_output()->chunk_count(),
                       _table_tpch_lineitems_scanned->get_output()->chunk_count()));
}

TEST_F(JoinHashTest, MaterializeInput) {
  std::vector<std::vector<size_t>> histograms;
  auto radix_container =
      materialize_input<int, int>(_table_tpch_lineitems_scanned->get_output(), ColumnID{0}, histograms, 0);

  // When radix bit count == 0, only one cluster is created which thus holds all elements.
  EXPECT_EQ(radix_container.elements->size(), _table_tpch_lineitems_scanned->get_output()->row_count());
}

TEST_F(JoinHashTest, MaterializeAndBuildWithKeepNulls) {
  size_t radix_bit_count = 0;
  std::vector<std::vector<size_t>> histograms;

  // we materialize the table twice, once with keeping NULL values and once without
  auto materialized_with_nulls =
      materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, true);
  auto materialized_without_nulls =
      materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, false);

  // Note: due to initialization with empty Partition Elements, NULL values are not materialized but
  // size of materialized input does not shrink due to NULL values.
  EXPECT_EQ(materialized_with_nulls.elements->size(), materialized_without_nulls.elements->size());
  // check if NULL values have been ignored
  EXPECT_EQ(materialized_without_nulls.elements->at(6).value, 9);
  EXPECT_EQ(materialized_with_nulls.elements->at(6).value, 13);

  // build phase: NULLs we be discarded
  auto hash_map_with_nulls = build<int, int>(materialized_with_nulls);
  auto hash_map_without_nulls = build<int, int>(materialized_without_nulls);

  // check for the expected number of hash maps
  EXPECT_EQ(hash_map_with_nulls.size(), pow(2, radix_bit_count));
  EXPECT_EQ(hash_map_without_nulls.size(), pow(2, radix_bit_count));

  // get count of non-NULL values in table
  auto table_without_nulls_scanned =
      create_table_scan(_table_with_nulls, ColumnID{0}, PredicateCondition::IsNotNull, 0);
  table_without_nulls_scanned->execute();

  // now that build removed the unneeded init values, map sizes should differ
  EXPECT_EQ(
      this->get_row_count(hash_map_without_nulls.at(0).value().begin(), hash_map_without_nulls.at(0).value().end()),
      table_without_nulls_scanned->get_output()->row_count());
}

TEST_F(JoinHashTest, MaterializeInputHistograms) {
  std::vector<std::vector<size_t>> histograms;

  // When using 1 bit for radix partitioning, we have two radix clusters determined on the least
  // significant bit. For the 0/1 table, we should thus cluster the ones and the zeros.
  materialize_input<int, int>(_table_zero_one, ColumnID{0}, histograms, 1);
  size_t histogram_offset_sum = 0;
  for (const auto& radix_count_per_chunk : histograms) {
    for (auto count : radix_count_per_chunk) {
      EXPECT_EQ(count, this->_table_size_zero_one / 2);
      histogram_offset_sum += count;
    }
  }
  EXPECT_EQ(histogram_offset_sum, _table_zero_one->row_count());

  histograms.clear();

  // When using 2 bits for radix partitioning, we have four radix clusters determined on the two least
  // significant bits. For the 0/1 table, we expect two non-empty clusters (00/01) and two empty ones (10/11).
  // Since the radix clusters are determine by hashing the value, we do not know in which cluster
  // the values are going to be stored.
  size_t empty_cluster_count = 0;
  materialize_input<int, int>(_table_zero_one, ColumnID{0}, histograms, 2);
  for (const auto& radix_count_per_chunk : histograms) {
    for (auto count : radix_count_per_chunk) {
      // Againg: due to the hashing, we do not know which cluster holds the value
      // But we know that two buckets have tab _table_size_zero_one/2 items and two none items.
      EXPECT_TRUE(count == this->_table_size_zero_one / 2 || count == 0);
      if (count == 0) ++empty_cluster_count;
    }
  }
  EXPECT_EQ(empty_cluster_count, 2);
}

}  // namespace opossum
