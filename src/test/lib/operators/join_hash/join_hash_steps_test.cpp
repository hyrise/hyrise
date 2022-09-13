#include <algorithm>

#include "base_test.hpp"

#include "operators/join_hash/join_hash_steps.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"

namespace hyrise {

/*
  The purpose of this test case is to test the single steps of the Hash Join (e.g., build(), probe(), etc.).
*/

class JoinHashStepsTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);
    _table_zero_one = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size_zero_one);
    for (auto i = size_t{0}; i < _table_size_zero_one; ++i) {
      _table_zero_one->append({static_cast<int>(i % 2)});
    }

    _table_int_with_nulls =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float_with_null.tbl", ChunkOffset{10}));
    _table_int_with_nulls->never_clear_output();
    _table_int_with_nulls->execute();

    _table_with_nulls_and_zeros =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int4_with_null.tbl", ChunkOffset{10}));
    _table_with_nulls_and_zeros->never_clear_output();
    _table_with_nulls_and_zeros->execute();

    // filter retains all rows
    _table_with_nulls_and_zeros_scanned =
        create_table_scan(_table_with_nulls_and_zeros, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_with_nulls_and_zeros_scanned->never_clear_output();
    _table_with_nulls_and_zeros_scanned->execute();
  }

  void SetUp() override {}

  // Accumulates the RowIDs hidden behind the iterator element (hash map stores PosLists, not RowIDs)
  template <typename Iter>
  size_t get_row_count(Iter begin, Iter end) {
    size_t row_count = 0;
    for (Iter iter = begin; iter != end; ++iter) {
      row_count += iter->size();
    }
    return row_count;
  }

  inline static auto _table_size_zero_one = ChunkOffset{1'000};
  inline static auto _chunk_size_zero_one = ChunkOffset{10};
  inline static std::shared_ptr<Table> _table_zero_one;
  inline static std::shared_ptr<TableWrapper> _table_int_with_nulls, _table_with_nulls_and_zeros;
  inline static std::shared_ptr<TableScan> _table_with_nulls_and_zeros_scanned;
};

TEST_F(JoinHashStepsTest, SmallHashTableAllPositions) {
  auto table = PosHashTable<int64_t>{JoinHashBuildMode::AllPositions, 50};
  for (auto index = uint32_t{0}; index < 10; ++index) {
    table.emplace(int64_t{index}, RowID{ChunkID{100u + index}, ChunkOffset{200u + index}});
    table.emplace(int64_t{index}, RowID{ChunkID{100u + index}, ChunkOffset{200u + index + 1}});
  }
  const auto expected_pos_list =
      RowIDPosList{RowID{ChunkID{105}, ChunkOffset{205}}, RowID{ChunkID{105}, ChunkOffset{206}}};

  table.finalize();
  {
    EXPECT_TRUE(table.contains(5));
    EXPECT_FALSE(table.contains(1000));
    auto [iter, end] = table.find(5);
    const auto materialized_result = RowIDPosList{iter, end};
    EXPECT_EQ(materialized_result, expected_pos_list);
  }
}

TEST_F(JoinHashStepsTest, LargeHashTableExistenceOnly) {
  auto table = PosHashTable<int64_t>{JoinHashBuildMode::ExistenceOnly, 100};
  for (auto index = uint32_t{0}; index < 100; ++index) {
    table.emplace(int64_t{index}, RowID{ChunkID{100u + index}, ChunkOffset{200u + index}});
    table.emplace(int64_t{index}, RowID{ChunkID{100u + index}, ChunkOffset{200u + index + 1}});
  }
  const auto expected_pos_list = RowIDPosList{RowID{ChunkID{150}, ChunkOffset{250}}};
  {
    EXPECT_TRUE(table.contains(5));
    EXPECT_FALSE(table.contains(1000));
  }
  table.finalize();
  {
    EXPECT_TRUE(table.contains(5));
    EXPECT_FALSE(table.contains(1000));
  }
}

TEST_F(JoinHashStepsTest, MaterializeAndBuildWithKeepNulls) {
  const size_t radix_bit_count = 0;
  std::vector<std::vector<size_t>> histograms;

  // BloomFilters are ignored in this test
  BloomFilter bloom_filter_with_nulls;
  BloomFilter bloom_filter_without_nulls;

  // We materialize the table twice, once with keeping NULL values and once without
  auto materialized_with_nulls = materialize_input<int, int, true>(
      _table_with_nulls_and_zeros->get_output(), ColumnID{0}, histograms, radix_bit_count, bloom_filter_with_nulls);
  auto materialized_without_nulls = materialize_input<int, int, false>(
      _table_with_nulls_and_zeros->get_output(), ColumnID{0}, histograms, radix_bit_count, bloom_filter_without_nulls);

  // Partition count should be equal to chunk count
  EXPECT_EQ(materialized_with_nulls.size(),
            static_cast<size_t>(_table_with_nulls_and_zeros->get_output()->chunk_count()));
  EXPECT_EQ(materialized_without_nulls.size(),
            static_cast<size_t>(_table_with_nulls_and_zeros->get_output()->chunk_count()));

  // Sum of partition sizes should be equal to row count if NULL values are contained and lower if they are not
  auto materialized_with_nulls_size = size_t{0};
  for (const auto& partition : materialized_with_nulls) {
    materialized_with_nulls_size += partition.elements.size();
  }
  EXPECT_EQ(materialized_with_nulls_size, _table_with_nulls_and_zeros->get_output()->row_count());

  auto materialized_without_nulls_size = size_t{0};
  for (const auto& partition : materialized_without_nulls) {
    materialized_without_nulls_size += partition.elements.size();
  }
  EXPECT_LT(materialized_without_nulls_size, _table_with_nulls_and_zeros->get_output()->row_count());

  // Check for values being properly set
  EXPECT_EQ(materialized_without_nulls[0].elements.at(6).value, 9);
  EXPECT_EQ(materialized_with_nulls[0].elements.at(6).value, 13);

  // Check for NULL values
  for (const auto& partition : materialized_without_nulls) {
    EXPECT_EQ(partition.null_values.size(), size_t{0});
  }

  // For materialized_with_nulls, NULLs should be set according to the data in the segment
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < _table_with_nulls_and_zeros->get_output()->chunk_count(); ++chunk_id) {
    const auto segment = _table_with_nulls_and_zeros->get_output()->get_chunk(chunk_id)->get_segment(ColumnID{0});

    resolve_data_and_segment_type(*segment, [&](auto type, auto& typed_segment) {
      using Type = typename decltype(type)::type;
      auto iterable = create_iterable_from_segment<Type>(typed_segment);

      size_t counter = 0;
      iterable.with_iterators([&](auto it, auto end) {
        for (; it != end; ++it) {
          const bool null_flag = materialized_with_nulls[chunk_id].null_values[counter++];
          EXPECT_EQ(null_flag, it->is_null());
        }
      });
    });
  }
}

TEST_F(JoinHashStepsTest, MaterializeOutputBloomFilter) {
  {
    std::vector<std::vector<size_t>> histograms;  // Ignored in this test
    BloomFilter bloom_filter;

    materialize_input<int, int, false>(_table_with_nulls_and_zeros->get_output(), ColumnID{0}, histograms, 1,
                                       bloom_filter);

    // For all input values, their position in the bloom filter should be correctly set to true
    for (auto value : std::vector<int>{0, 6, 7, 9, 13, 18}) {
      EXPECT_TRUE(bloom_filter[value]);

      // Set to false for easier checking of the values that should be false
      bloom_filter[value] = false;
    }

    // All other slots should be false
    EXPECT_EQ(bloom_filter, BloomFilter{BLOOM_FILTER_SIZE});
  }
}

TEST_F(JoinHashStepsTest, MaterializeInputBloomFilter) {
  {
    std::vector<std::vector<size_t>> histograms;  // Ignored in this test
    BloomFilter output_bloom_filter;

    // Fill input_bloom_filter
    BloomFilter input_bloom_filter(BLOOM_FILTER_SIZE);
    for (auto value : std::vector<int>{6, 7, 9}) {
      input_bloom_filter[value] = true;
    }

    auto container = materialize_input<int, int, false>(_table_with_nulls_and_zeros->get_output(), ColumnID{0},
                                                        histograms, 1, output_bloom_filter, input_bloom_filter);

    auto materialized_values = std::vector<int>{};
    auto chunk_offsets = std::vector<int>{};

    for (const auto& partition : container) {
      for (const auto& element : partition.elements) {
        materialized_values.emplace_back(element.value);
        chunk_offsets.emplace_back(static_cast<int>(element.row_id.chunk_offset));
      }
    }

    const auto expected_values = std::vector<int>{7, 7, 9, 6, 9, 7};
    const auto expected_offsets = std::vector<int>{1, 2, 3, 4, 8, 9};

    EXPECT_EQ(materialized_values, expected_values);
    EXPECT_EQ(chunk_offsets, expected_offsets);
  }
}

TEST_F(JoinHashStepsTest, MaterializeInputHistograms) {
  {
    std::vector<std::vector<size_t>> histograms;
    BloomFilter bloom_filter;  // Ignored in this test

    // When using 1 bit for radix partitioning, we have two radix clusters determined on the least
    // significant bit. For the 0/1 table, we should thus cluster the ones and the zeros.
    materialize_input<int, int, false>(_table_zero_one, ColumnID{0}, histograms, 1, bloom_filter);
    size_t histogram_offset_sum = 0;
    EXPECT_EQ(histograms.size(), this->_table_size_zero_one / this->_chunk_size_zero_one);
    for (const auto& radix_count_per_chunk : histograms) {
      for (auto count : radix_count_per_chunk) {
        EXPECT_EQ(count, this->_chunk_size_zero_one / 2);
        histogram_offset_sum += count;
      }
    }
    EXPECT_EQ(histogram_offset_sum, _table_zero_one->row_count());
  }

  {
    std::vector<std::vector<size_t>> histograms;
    BloomFilter bloom_filter;  // Ignored in this test

    // When using 2 bits for radix partitioning, we have four radix clusters determined on the two least
    // significant bits. For the 0/1 table, we expect two non-empty clusters (00/01) and two empty ones (10/11).
    // Since the radix clusters are determine by hashing the value, we do not know in which cluster
    // the values are going to be stored.
    size_t empty_cluster_count = 0;
    materialize_input<int, int, false>(_table_zero_one, ColumnID{0}, histograms, 2, bloom_filter);
    for (const auto& radix_count_per_chunk : histograms) {
      for (auto count : radix_count_per_chunk) {
        // Again, due to the hashing, we do not know which cluster holds the value
        // But we know that two buckets have _table_size_zero_one/2 items and two have none items.
        EXPECT_TRUE(count == this->_chunk_size_zero_one / 2 || count == 0);
        if (count == 0) {
          ++empty_cluster_count;
        }
      }
    }
    EXPECT_EQ(empty_cluster_count, 2 * this->_table_size_zero_one / this->_chunk_size_zero_one);
  }
}

TEST_F(JoinHashStepsTest, RadixClusteringOfNulls) {
  const size_t radix_bit_count = 1;
  std::vector<std::vector<size_t>> histograms;
  BloomFilter bloom_filter;  // Ignored in this test

  const auto materialized_without_null_handling = materialize_input<int, int, true>(
      _table_int_with_nulls->get_output(), ColumnID{0}, histograms, radix_bit_count, bloom_filter);
  // Ensure we created NULL value information
  EXPECT_EQ(materialized_without_null_handling[0].null_values.size(),
            materialized_without_null_handling[0].elements.size());

  const auto radix_cluster_result =
      partition_by_radix<int, int, true>(materialized_without_null_handling, histograms, radix_bit_count);

  // Loaded table does not include int=0 values, so all int=0 values are NULLs
  for (const auto& partition : radix_cluster_result) {
    for (auto i = size_t{0}; i < partition.elements.size(); ++i) {
      const auto value = partition.elements.at(i).value;
      const bool null_flag = partition.null_values[i++];
      if (value == 0) {
        EXPECT_TRUE(null_flag);
      } else {
        EXPECT_FALSE(null_flag);
      }
    }
  }
}

TEST_F(JoinHashStepsTest, BuildRespectsBloomFilter) {
  std::vector<std::vector<size_t>> histograms;  // Ignored in this test
  BloomFilter output_bloom_filter;              // Ignored in this test

  // Fill input_bloom_filter
  BloomFilter input_bloom_filter(BLOOM_FILTER_SIZE);
  for (auto value : std::vector<int>{6, 7, 9}) {
    input_bloom_filter[value] = true;
  }

  auto container = materialize_input<int, int, false>(_table_with_nulls_and_zeros->get_output(), ColumnID{0},
                                                      histograms, 1, output_bloom_filter);

  auto hash_tables = build<int, int>(container, JoinHashBuildMode::AllPositions, 0, input_bloom_filter);

  EXPECT_EQ(hash_tables.size(), 1);
  const auto& hash_table = hash_tables[0];
  EXPECT_TRUE(hash_table);

  EXPECT_TRUE(hash_table->contains(6));
  EXPECT_TRUE(hash_table->contains(7));
  EXPECT_TRUE(hash_table->contains(9));
  EXPECT_FALSE(hash_table->contains(13));
  EXPECT_FALSE(hash_table->contains(18));
}

TEST_F(JoinHashStepsTest, ThrowWhenNoNullValuesArePassed) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  auto radix_bit_count = size_t{0};
  auto histograms = std::vector<std::vector<size_t>>{};
  BloomFilter bloom_filter;  // Ignored in this test

  const auto materialized_without_null_handling = materialize_input<int, int, false>(
      _table_with_nulls_and_zeros->get_output(), ColumnID{0}, histograms, radix_bit_count, bloom_filter);
  // We want to test a non-NULL-considering Radix Container, ensure we did it correctly
  EXPECT_EQ(materialized_without_null_handling[0].null_values.size(), 0);

  // Using true as the NULL handing flag should lead to an error,
  // because we did not create NULL value information during materialization.
  // Note, the extra parantheses are required for Gtest since otherwise the preprocessor
  // has problems resolving this code line (see https://stackoverflow.com/a/35957776/1147726)
  EXPECT_THROW((partition_by_radix<int, int, true>)(materialized_without_null_handling, histograms, radix_bit_count),
               std::logic_error);
}

}  // namespace hyrise
