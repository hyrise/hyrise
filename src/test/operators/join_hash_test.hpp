#pragma once

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/join_hash.cpp"  // to access free functions build() etc.
#include "operators/join_hash.hpp"
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
        std::make_shared<TableScan>(_table_tpch_orders, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_orders_scanned->execute();
    _table_tpch_lineitems_scanned =
        std::make_shared<TableScan>(_table_tpch_lineitems, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_lineitems_scanned->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems, _table_with_nulls;
  std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;

  template <typename Iter>
  size_t get_number_of_rows(Iter begin, Iter end) {
    size_t row_count = 0;
    for (Iter it = begin; it != end; ++it) {
      auto value = it->second;
      if (value.type() == typeid(RowID)) {
        row_count += 1;
      } else {
        row_count += boost::get<PosList>(value).size();
      }
    }
    return row_count;
  }

  template <typename T, typename HashType>
  void test_hash_map(const std::vector<T>& values) {
    std::vector<PartitionedElement<T>> elements;
    for (size_t i = 0; i < values.size(); ++i) {
      RowID row_id{ChunkID{17}, ChunkOffset{static_cast<unsigned int>(i)}};
      elements.emplace_back(PartitionedElement<T>{row_id, 17, static_cast<T>(values.at(i))});
    }

    auto hash_map = build<T, HashType>(RadixContainer<T>{std::make_shared<std::vector<PartitionedElement<T>>>(elements),
                                                         std::vector<size_t>{elements.size()}});

    // With only one offset value passed, one hash map will be created
    EXPECT_EQ(hash_map.size(), 1);

    auto row_count = get_number_of_rows(hash_map.at(0)->begin(), hash_map.at(0)->end());
    EXPECT_EQ(row_count, elements.size());

    ASSERT_TRUE(hash_map.at(0).has_value());  // hash map for first (and only) chunk exists

    unsigned int counter = 0;
    for (const auto& element : elements) {
      auto probe_value = element.value;

      auto result = hash_map.at(0).value().at(probe_value);
      if (result.type() == typeid(RowID)) {
        EXPECT_EQ(boost::get<RowID>(result), element.row_id);
      } else {
        auto result_list = boost::get<PosList>(result);
        RowID probe_row_id{ChunkID{17}, ChunkOffset{static_cast<unsigned int>(counter)}};
        EXPECT_TRUE(std::find(result_list.begin(), result_list.end(), probe_row_id) != result_list.end());
      }
      ++counter;
    }
  }
};

TEST_F(JoinHashTest, OperatorName) {
  auto join = std::make_shared<JoinHash>(_table_wrapper_small, _table_wrapper_small, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);

  EXPECT_EQ(join->name(), "JoinHash");
}

TEST_F(JoinHashTest, ChunkCount) {
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
  std::vector<std::shared_ptr<std::vector<size_t>>> histograms;
  auto radix_container =
      materialize_input<int, int>(_table_tpch_lineitems_scanned->get_output(), ColumnID{0}, histograms, 0, 17);

  EXPECT_EQ(radix_container.elements->size(), _table_tpch_lineitems_scanned->get_output()->row_count());
}

TEST_F(JoinHashTest, MaterializeAndBuildWithKeepNulls) {
  size_t radix_bit_count = 0;
  std::vector<std::shared_ptr<std::vector<size_t>>> histograms;

  auto table_without_nulls_scanned =
      std::make_shared<TableScan>(_table_with_nulls, ColumnID{0}, PredicateCondition::IsNotNull, 0);
  table_without_nulls_scanned->execute();

  auto mat_with_nulls =
      materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, 17, true);
  auto mat_without_nulls =
      materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, 17, false);

  // Note: due to initialization with empty Partition Elements, NULL values are not materialized but
  // size of materialized input does not shrink due to NULL values.
  EXPECT_EQ(mat_with_nulls.elements->size(), mat_without_nulls.elements->size());

  // build phase
  auto hash_map_with_nulls = build<int, int>(mat_with_nulls);
  auto hash_map_without_nulls = build<int, int>(mat_without_nulls);

  EXPECT_EQ(hash_map_with_nulls.size(), pow(2, radix_bit_count));
  EXPECT_EQ(hash_map_without_nulls.size(), pow(2, radix_bit_count));

  // now that build removed the invalid values, map sizes should differ
  auto row_count = this->get_number_of_rows(hash_map_without_nulls.at(0)->begin(), hash_map_without_nulls.at(0)->end());
  EXPECT_EQ(row_count, table_without_nulls_scanned->get_output()->row_count());
}

TEST_F(JoinHashTest, MaterializeInputHistograms) {
  std::vector<std::shared_ptr<std::vector<size_t>>> histograms;
  materialize_input<int, int>(_table_tpch_lineitems_scanned->get_output(), ColumnID{0}, histograms, 0, 17);

  size_t histogram_offset_sum = 0;
  for (const auto& radix_count_per_chunk : histograms) {
    for (auto count : *radix_count_per_chunk) {
      histogram_offset_sum += count;
    }
  }

  EXPECT_EQ(histogram_offset_sum, _table_tpch_lineitems_scanned->get_output()->row_count());
}

}  // namespace opossum
