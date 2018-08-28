#include <type_traits>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "resolve_type.hpp"
#include "operators/table_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_hash.cpp"  // to access free functions build() etc.
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

    _table_tpch_orders = std::make_shared<TableWrapper>(load_table("src/test/tables/tpch/sf-0.001/orders.tbl", 10));
    _table_tpch_orders->execute();

    _table_tpch_lineitems = std::make_shared<TableWrapper>(load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl", 10));
    _table_tpch_lineitems->execute();

    _table_with_nulls = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int4_with_null.tbl", 10));
    _table_with_nulls->execute();

    // filters retains all rows
    _table_tpch_orders_scanned = std::make_shared<TableScan>(_table_tpch_orders, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_orders_scanned->execute();
    _table_tpch_lineitems_scanned = std::make_shared<TableScan>(_table_tpch_lineitems, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_lineitems_scanned->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems, _table_with_nulls;
  std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;
};

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
  auto radix_container = materialize_input<int, int>(_table_tpch_lineitems_scanned->get_output(), ColumnID{0}, histograms, 0, 17);

  EXPECT_EQ(radix_container.elements->size(), _table_tpch_lineitems_scanned->get_output()->row_count());
}

TEST_F(JoinHashTest, MaterializeAndBuildWithKeepNulls) {
  size_t radix_bit_count = 0;
  std::vector<std::shared_ptr<std::vector<size_t>>> histograms;

  auto table_without_nulls_scanned = std::make_shared<TableScan>(_table_with_nulls, ColumnID{0}, PredicateCondition::IsNotNull, 0);
  table_without_nulls_scanned->execute();

  auto mat_with_nulls = materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, 17, true);
  auto mat_without_nulls = materialize_input<int, int>(_table_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, 17, false);

  // Note: due to initialization with empty Partition Elements, NULL values are not materialized but
  // size of materialized input does not shrink due to NULL values.
  EXPECT_EQ(mat_with_nulls.elements->size(), mat_without_nulls.elements->size());

  // build phase
  auto hash_map_with_nulls = build<int, int>(mat_with_nulls);
  auto hash_map_without_nulls = build<int, int>(mat_without_nulls);

  EXPECT_EQ(hash_map_with_nulls.size(), pow(2, radix_bit_count));
  EXPECT_EQ(hash_map_without_nulls.size(), pow(2, radix_bit_count));

  // now that build removed the invalid values, map sizes should differ
  auto row_count = get_number_of_rows(hash_map_without_nulls.at(0)->begin(), hash_map_without_nulls.at(0)->end());
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
