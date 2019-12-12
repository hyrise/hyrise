#include "../base_test.hpp"

#include "operators/join_hash/join_hash_steps.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"

namespace opossum {

/*
  The purpose of this test case is to test the single steps of the Hash Join (e.g., build(), probe() , etc.).
*/

class JoinHashStepsTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_size_zero_one = 1'000;

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);
    _table_zero_one = std::make_shared<Table>(column_definitions, TableType::Data, _table_size_zero_one);
    for (auto i = size_t{0}; i < _table_size_zero_one; ++i) {
      _table_zero_one->append({static_cast<int>(i % 2)});
    }

    _table_int_with_nulls =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float_with_null.tbl", 10));
    _table_int_with_nulls->execute();

    _table_with_nulls_and_zeros =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int4_with_null.tbl", 10));
    _table_with_nulls_and_zeros->execute();

    // filter retains all rows
    _table_with_nulls_and_zeros_scanned =
        create_table_scan(_table_with_nulls_and_zeros, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_with_nulls_and_zeros_scanned->execute();
  }

  void SetUp() override {}

  // Accumulates the RowIDs hidden behind the iterator element (hash map stores PosLists, not RowIDs)
  template <typename Iter>
  size_t get_row_count(Iter begin, Iter end) {
    size_t row_count = 0;
    for (Iter it = begin; it != end; ++it) {
      row_count += it->size();
    }
    return row_count;
  }

  inline static size_t _table_size_zero_one = 0;
  inline static std::shared_ptr<Table> _table_zero_one;
  inline static std::shared_ptr<TableWrapper> _table_int_with_nulls, _table_with_nulls_and_zeros;
  inline static std::shared_ptr<TableScan> _table_with_nulls_and_zeros_scanned;
};

TEST_F(JoinHashStepsTest, SmallHashTableAllPositions) {
  auto table = PosHashTable<int>{JoinHashBuildMode::AllPositions, 50};
  for (auto i = 0; i < 10; ++i) {
    table.emplace(i, RowID{ChunkID{ChunkID::base_type{100} + i}, ChunkOffset{200} + i});
    table.emplace(i, RowID{ChunkID{ChunkID::base_type{100} + i}, ChunkOffset{200} + i + 1});
  }
  const auto expected_pos_list = boost::container::small_vector<RowID, 1>{RowID{ChunkID{105}, ChunkOffset{205}},
                                                                          RowID{ChunkID{105}, ChunkOffset{206}}};
  {
    EXPECT_TRUE(table.contains(5));
    EXPECT_FALSE(table.contains(1000));
    const auto pos_list = *table.find(5);
    EXPECT_EQ(pos_list, expected_pos_list);
  }
  table.shrink_to_fit();
  {
    EXPECT_TRUE(table.contains(5));
    EXPECT_FALSE(table.contains(1000));
    const auto pos_list = *table.find(5);
    EXPECT_EQ(pos_list, expected_pos_list);
  }
}

TEST_F(JoinHashStepsTest, LargeHashTableSinglePositions) {
  auto table = PosHashTable<int>{JoinHashBuildMode::SinglePosition, 100};
  for (auto i = 0; i < 100; ++i) {
    table.emplace(i, RowID{ChunkID{ChunkID::base_type{100} + i}, ChunkOffset{200} + i});
    table.emplace(i, RowID{ChunkID{ChunkID::base_type{100} + i}, ChunkOffset{200} + i + 1});
  }
  const auto expected_pos_list = boost::container::small_vector<RowID, 1>{RowID{ChunkID{150}, ChunkOffset{250}}};
  {
    EXPECT_TRUE(table.contains(5));
    EXPECT_FALSE(table.contains(1000));
    const auto pos_list = *table.find(50);
    EXPECT_EQ(pos_list, expected_pos_list);
  }
  table.shrink_to_fit();
  {
    EXPECT_TRUE(table.contains(5));
    EXPECT_FALSE(table.contains(1000));
    const auto pos_list = *table.find(50);
    EXPECT_EQ(pos_list, expected_pos_list);
  }
}

TEST_F(JoinHashStepsTest, MaterializeInput) {
  std::vector<std::vector<size_t>> histograms;
  const auto chunk_offsets = determine_chunk_offsets(_table_with_nulls_and_zeros_scanned->get_output());
  auto radix_container = materialize_input<int, int, false>(_table_with_nulls_and_zeros_scanned->get_output(),
                                                            ColumnID{0}, chunk_offsets, histograms, 0);

  // When radix bit count == 0, only one cluster is created which thus holds all elements.
  EXPECT_EQ(radix_container.elements->size(), _table_with_nulls_and_zeros_scanned->get_output()->row_count());
}

TEST_F(JoinHashStepsTest, MaterializeAndBuildWithKeepNulls) {
  size_t radix_bit_count = 0;
  std::vector<std::vector<size_t>> histograms;

  const auto chunk_offsets = determine_chunk_offsets(_table_with_nulls_and_zeros->get_output());

  // We materialize the table twice, once with keeping NULL values and once without
  auto materialized_with_nulls = materialize_input<int, int, true>(
      _table_with_nulls_and_zeros->get_output(), ColumnID{0}, chunk_offsets, histograms, radix_bit_count);
  auto materialized_without_nulls = materialize_input<int, int, false>(
      _table_with_nulls_and_zeros->get_output(), ColumnID{0}, chunk_offsets, histograms, radix_bit_count);

  // Note: due to initialization with empty Partition Elements, NULL values are not materialized but
  // the resulting size of the materialized input does not shrink due to NULL values (i.e., it's still
  // the size of the pre-allocated vectors).
  EXPECT_EQ(materialized_with_nulls.elements->size(), materialized_without_nulls.elements->size());
  EXPECT_EQ(materialized_with_nulls.elements->size(), _table_with_nulls_and_zeros->get_output()->row_count());
  // check if NULL values have been ignored
  EXPECT_EQ(materialized_without_nulls.elements->at(6).value, 9);
  EXPECT_EQ(materialized_with_nulls.elements->at(6).value, 13);

  EXPECT_EQ(materialized_with_nulls.null_value_bitvector->size(), materialized_with_nulls.elements->size());
  EXPECT_EQ(materialized_without_nulls.null_value_bitvector->size(), 0);

  // The main difference is that the vector storing NULL
  // value flags should be set. Test if bits are set.
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < _table_with_nulls_and_zeros->get_output()->chunk_count(); ++chunk_id) {
    const auto segment = _table_with_nulls_and_zeros->get_output()->get_chunk(chunk_id)->get_segment(ColumnID{0});

    resolve_data_and_segment_type(*segment, [&](auto type, auto& typed_segment) {
      using Type = typename decltype(type)::type;
      auto iterable = create_iterable_from_segment<Type>(typed_segment);

      size_t counter = 0;
      iterable.with_iterators([&](auto it, auto end) {
        for (; it != end; ++it) {
          const bool null_flag = (*materialized_with_nulls.null_value_bitvector)[counter++];
          EXPECT_EQ(null_flag, it->is_null());
        }
      });
    });
  }

  // build phase: NULLs we be discarded
  auto hash_map_with_nulls = build<int, int>(materialized_with_nulls, JoinHashBuildMode::AllPositions);
  auto hash_map_without_nulls = build<int, int>(materialized_without_nulls, JoinHashBuildMode::AllPositions);

  // check for the expected number of hash maps
  EXPECT_EQ(hash_map_with_nulls.size(), pow(2, radix_bit_count));
  EXPECT_EQ(hash_map_without_nulls.size(), pow(2, radix_bit_count));

  // get count of non-NULL values in table
  auto table_without_nulls_scanned =
      create_table_scan(_table_with_nulls_and_zeros, ColumnID{0}, PredicateCondition::IsNotNull, 0);
  table_without_nulls_scanned->execute();

  // now that build removed the unneeded init values, map sizes should differ
  EXPECT_EQ(
      this->get_row_count(hash_map_without_nulls.at(0).value().begin(), hash_map_without_nulls.at(0).value().end()),
      table_without_nulls_scanned->get_output()->row_count());
}

TEST_F(JoinHashStepsTest, MaterializeInputHistograms) {
  std::vector<std::vector<size_t>> histograms;

  const auto chunk_offsets = determine_chunk_offsets(_table_zero_one);

  // When using 1 bit for radix partitioning, we have two radix clusters determined on the least
  // significant bit. For the 0/1 table, we should thus cluster the ones and the zeros.
  materialize_input<int, int, false>(_table_zero_one, ColumnID{0}, chunk_offsets, histograms, 1);
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
  materialize_input<int, int, false>(_table_zero_one, ColumnID{0}, chunk_offsets, histograms, 2);
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

TEST_F(JoinHashStepsTest, RadixClusteringOfNulls) {
  size_t radix_bit_count = 1;
  std::vector<std::vector<size_t>> histograms;

  const auto chunk_offsets = determine_chunk_offsets(_table_int_with_nulls->get_output());

  const auto materialized_without_null_handling = materialize_input<int, int, true>(
      _table_int_with_nulls->get_output(), ColumnID{0}, chunk_offsets, histograms, radix_bit_count);
  // Ensure we created NULL value information
  EXPECT_EQ(materialized_without_null_handling.null_value_bitvector->size(),
            materialized_without_null_handling.elements->size());

  const auto radix_cluster_result = partition_radix_parallel<int, int, true>(
      materialized_without_null_handling, chunk_offsets, histograms, radix_bit_count);

  // Loaded table does not include int=0 values, so all int=0 values are NULLs
  for (auto i = size_t{0}; i < radix_cluster_result.elements->size(); ++i) {
    const auto value = radix_cluster_result.elements->at(i).value;
    const bool null_flag = (*radix_cluster_result.null_value_bitvector)[i++];
    if (value == 0) {
      EXPECT_TRUE(null_flag);
    } else {
      EXPECT_FALSE(null_flag);
    }
  }
}

TEST_F(JoinHashStepsTest, DetermineChunkOffsets) {
  // offset store the start offset for each chunk
  const auto chunk_offsets_nulls = determine_chunk_offsets(_table_with_nulls_and_zeros->get_output());
  EXPECT_EQ(chunk_offsets_nulls.size(), 2);
  EXPECT_EQ(chunk_offsets_nulls[0], 0);
  EXPECT_EQ(chunk_offsets_nulls[1], 10);
}

TEST_F(JoinHashStepsTest, ThrowWhenNoNullValuesArePassed) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  size_t radix_bit_count = 0;
  std::vector<std::vector<size_t>> histograms;

  const auto chunk_offsets = determine_chunk_offsets(_table_with_nulls_and_zeros->get_output());

  const auto materialized_without_null_handling = materialize_input<int, int, false>(
      _table_with_nulls_and_zeros->get_output(), ColumnID{0}, chunk_offsets, histograms, radix_bit_count);
  // We want to test a non-NULL-considering Radix Container, ensure we did it correctly
  EXPECT_EQ(materialized_without_null_handling.null_value_bitvector->size(), 0);

  // Using true as the NULL handing flag should lead to an error,
  // because we did not create NULL value information during materialization.
  // Note, the extra parantheses are required for Gtest since otherwise the preprocessor
  // has problems resolving this code line (see https://stackoverflow.com/a/35957776/1147726)
  EXPECT_THROW((partition_radix_parallel<int, int, true>)(materialized_without_null_handling, chunk_offsets, histograms,
                                                          radix_bit_count),
               std::logic_error);
}

}  // namespace opossum
