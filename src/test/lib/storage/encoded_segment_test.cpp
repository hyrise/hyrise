#include <cctype>
#include <memory>
#include <sstream>

#include "base_test.hpp"
#include "lib/storage/encoding_test.hpp"

#include "constant_mappings.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_access_counter.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"

#include "types.hpp"

namespace opossum {

class EncodedSegmentTest : public BaseTestWithParam<SegmentEncodingSpec> {
 protected:
  static constexpr auto _max_value = 1'024;

  // 0, 1, -1, _max_value, -_max_value and 20 randomly generated int32_t values from the interval
  // [-_max_value, _max_value]
  const std::array<int32_t, 25> _generated_value_pool{0,   1,    -1,   _max_value, -_max_value, 750, -978, -350, -885,
                                                      738, -234, 582,  269,        -485,        893, -630, 528,  -122,
                                                      551, 544,  -106, 194,        415,         954, -608};

 protected:
  size_t _row_count() {
    const auto encoding_spec = GetParam();
    return _row_count(encoding_spec.encoding_type);
  }

  size_t _row_count(EncodingType encoding_type) {
    static constexpr auto default_row_count = size_t{1u} << 10;

    switch (encoding_type) {
      case EncodingType::FrameOfReference:
        // fill three blocks and a bit more
        return static_cast<size_t>(FrameOfReferenceSegment<int32_t>::block_size * (3.3));
      default:
        return default_row_count;
    }
  }

  std::shared_ptr<ValueSegment<int32_t>> _create_int_value_segment() { return _create_int_value_segment(_row_count()); }

  std::shared_ptr<ValueSegment<int32_t>> _create_int_value_segment(size_t row_count) {
    auto values = pmr_vector<int32_t>(row_count);

    auto counter = 0u;
    for (auto& elem : values) {
      elem = _generated_value_pool[counter % _generated_value_pool.size()];
      ++counter;
    }

    return std::make_shared<ValueSegment<int32_t>>(std::move(values));
  }

  std::shared_ptr<ValueSegment<int32_t>> _create_int_with_null_value_segment() {
    return _create_int_with_null_value_segment(_row_count());
  }

  std::shared_ptr<ValueSegment<int32_t>> _create_int_with_null_value_segment(size_t row_count) {
    auto values = pmr_vector<int32_t>(row_count);
    auto null_values = pmr_vector<bool>(row_count);

    for (auto i = 0u; i < row_count; ++i) {
      values[i] = _generated_value_pool[i % _generated_value_pool.size()];
      null_values[i] = (i % 3) == 0;
    }

    return std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
  }

  std::shared_ptr<RowIDPosList> _create_sequential_position_filter() {
    return _create_sequential_position_filter(_row_count());
  }

  static std::shared_ptr<RowIDPosList> _create_sequential_position_filter(size_t row_count) {
    auto list = std::make_shared<RowIDPosList>();
    list->guarantee_single_chunk();

    for (auto offset_in_referenced_chunk = 0u; offset_in_referenced_chunk < row_count; ++offset_in_referenced_chunk) {
      if (offset_in_referenced_chunk % 2) {
        list->push_back(RowID{ChunkID{0}, offset_in_referenced_chunk});
      }
    }

    return list;
  }

  std::shared_ptr<RowIDPosList> _create_random_access_position_filter() {
    return _create_random_access_position_filter(_row_count());
  }

  static std::shared_ptr<RowIDPosList> _create_random_access_position_filter(size_t row_count) {
    auto list = _create_sequential_position_filter(row_count);

    auto skewed_list = std::make_shared<RowIDPosList>();
    skewed_list->guarantee_single_chunk();
    skewed_list->reserve(list->size());
    // Let one iterator run from the beginning and one from the end of the list in each other's direction. Add the
    // iterators' elements alternately to the skewed list.
    auto front_iter = list->cbegin();
    auto back_iter = list->cend() - 1;
    const auto half_list_size = list->size() / 2;
    for (auto counter = 0u; counter < half_list_size; ++counter) {
      skewed_list->emplace_back(std::move(*front_iter));
      skewed_list->emplace_back(std::move(*back_iter));
      ++front_iter;
      --back_iter;
    }
    if (front_iter == back_iter) {  // odd number of list elements
      skewed_list->emplace_back(std::move(*front_iter));
    }

    return skewed_list;
  }

  std::shared_ptr<AbstractEncodedSegment> _encode_segment(const std::shared_ptr<AbstractSegment>& abstract_segment,
                                                          const DataType data_type) {
    auto segment_encoding_spec = GetParam();
    return this->_encode_segment(abstract_segment, data_type, segment_encoding_spec);
  }

  std::shared_ptr<AbstractEncodedSegment> _encode_segment(const std::shared_ptr<AbstractSegment>& abstract_segment,
                                                          const DataType data_type,
                                                          const SegmentEncodingSpec& segment_encoding_spec) {
    return std::dynamic_pointer_cast<AbstractEncodedSegment>(
        ChunkEncoder::encode_segment(abstract_segment, data_type, segment_encoding_spec));
  }
};

auto encoded_segment_test_formatter = [](const ::testing::TestParamInfo<SegmentEncodingSpec> info) {
  const auto spec = info.param;

  auto stream = std::stringstream{};
  stream << spec.encoding_type;
  if (spec.vector_compression_type) {
    stream << "-" << *spec.vector_compression_type;
  }

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(SegmentEncodingSpecs, EncodedSegmentTest,
                         ::testing::ValuesIn(get_supporting_segment_encodings_specs(DataType::Int, false)),
                         encoded_segment_test_formatter);

TEST_P(EncodedSegmentTest, EncodeEmptyIntSegment) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{});
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  // Trying to iterate over the empty segments should not cause any errors or crashes.
  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators([&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
        // Nothing happens here since the segments are empty
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadNotNullableIntSegment) {
  auto value_segment = _create_int_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators([&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it) {
          EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
        }
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadNullableIntSegment) {
  auto value_segment = _create_int_with_null_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators([&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
        auto row_idx = 0u;
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it, ++row_idx) {
          // This covers `EncodedSegment::operator[]`
          if (variant_is_null((*value_segment)[row_idx])) {
            EXPECT_TRUE(variant_is_null(encoded_segment[row_idx]));
          } else {
            EXPECT_EQ((*value_segment)[row_idx], encoded_segment[row_idx]);
          }

          // This covers the point access iterator
          EXPECT_EQ(value_segment_it->is_null(), encoded_segment_it->is_null());

          if (!value_segment_it->is_null()) {
            EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadNullableIntSegmentWithChunkOffsetsList) {
  auto value_segment = _create_int_with_null_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  auto position_filter = _create_sequential_position_filter();

  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators(position_filter, [&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators(position_filter, [&](auto encoded_segment_it, auto encoded_segment_end) {
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it) {
          EXPECT_EQ(value_segment_it->is_null(), encoded_segment_it->is_null());

          if (!value_segment_it->is_null()) {
            EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadNullableIntSegmentWithShuffledChunkOffsetsList) {
  auto value_segment = _create_int_with_null_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  auto position_filter = _create_random_access_position_filter();

  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators(position_filter, [&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators(position_filter, [&](auto encoded_segment_it, auto encoded_segment_end) {
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it) {
          EXPECT_EQ(value_segment_it->is_null(), encoded_segment_it->is_null());

          if (!value_segment_it->is_null()) {
            EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadEmptyIntSegment) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{});
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  // Even if no actual reading happens here, iterators are created and we can test that they do not crash on empty
  // segments
  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
      // Nothing happens here since the segments are empty
    });
  });
}

TEST_F(EncodedSegmentTest, SeqIncreasingAccessValueSegmentCounters) {
  const auto row_count = 666u;
  auto value_segment = _create_int_value_segment(row_count);

  auto iterable = create_iterable_from_segment(*value_segment);
  EXPECT_EQ(0, value_segment->access_counter[SegmentAccessCounter::AccessType::Sequential]);
  iterable.for_each([](const auto) {});
  EXPECT_EQ(value_segment->size(), value_segment->access_counter[SegmentAccessCounter::AccessType::Sequential]);
}

TEST_P(EncodedSegmentTest, SeqIncreasingAccessSegmentCounters) {
  auto value_segment = _create_int_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);

  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto iterable = create_iterable_from_segment(encoded_segment);
    EXPECT_EQ(0, encoded_segment.access_counter[SegmentAccessCounter::AccessType::Sequential]);
    iterable.for_each([](const auto) {});
    EXPECT_EQ(encoded_segment.size(), encoded_segment.access_counter[SegmentAccessCounter::AccessType::Sequential]);
  });
}

TEST_P(EncodedSegmentTest, IncreasingSegmentAccessCountersWithPosList) {
  auto value_segment = _create_int_value_segment();
  const auto pos_filter = _create_sequential_position_filter();

  auto value_segment_iterable = create_iterable_from_segment(*value_segment);
  EXPECT_EQ(0, value_segment->access_counter[SegmentAccessCounter::AccessType::Monotonic]);
  value_segment_iterable.for_each(pos_filter, [](const auto) {});
  EXPECT_EQ(pos_filter->size(), value_segment->access_counter[SegmentAccessCounter::AccessType::Monotonic]);

  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);
  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto abstract_encoded_segment_iterable = create_iterable_from_segment(encoded_segment);
    EXPECT_EQ(0, encoded_segment.access_counter[SegmentAccessCounter::AccessType::Monotonic]);
    abstract_encoded_segment_iterable.for_each(pos_filter, [](const auto) {});
    EXPECT_EQ(pos_filter->size(), encoded_segment.access_counter[SegmentAccessCounter::AccessType::Monotonic]);
  });
}

TEST_P(EncodedSegmentTest, RandomSegmentAccessCountersWithPosList) {
  auto value_segment = _create_int_value_segment();
  const auto pos_filter = _create_random_access_position_filter();

  auto value_segment_iterable = create_iterable_from_segment(*value_segment);
  EXPECT_EQ(0, value_segment->access_counter[SegmentAccessCounter::AccessType::Random]);
  value_segment_iterable.for_each(pos_filter, [](const auto) {});
  EXPECT_EQ(pos_filter->size(), value_segment->access_counter[SegmentAccessCounter::AccessType::Random]);

  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::Int);
  resolve_encoded_segment_type<int32_t>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
    auto abstract_encoded_segment_iterable = create_iterable_from_segment(encoded_segment);
    EXPECT_EQ(0, encoded_segment.access_counter[SegmentAccessCounter::AccessType::Random]);
    abstract_encoded_segment_iterable.for_each(pos_filter, [](const auto) {});
    EXPECT_EQ(pos_filter->size(), encoded_segment.access_counter[SegmentAccessCounter::AccessType::Random]);
  });
}

TEST_P(EncodedSegmentTest, ScansIncrementCounters) {
  const auto input_table = load_table("resources/test_data/tbl/int_int_int.tbl");

  auto expected_access_counters = std::vector<SegmentAccessCounter>(3);
  const auto verify_access_counters = [&](const auto line) {
    const auto chunk_id = ChunkID{0};
    const auto& chunk = input_table->get_chunk(chunk_id);
    for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
      SCOPED_TRACE(std::string{"Chunk "} + std::to_string(chunk_id) + ", column " + std::to_string(column_id) +
                   " at line " + std::to_string(line));
      EXPECT_EQ(chunk->get_segment(column_id)->access_counter, expected_access_counters.at(column_id));
    }
  };

  // ValueSegment::append as called by load_table increases the access counters
  expected_access_counters[0][SegmentAccessCounter::AccessType::Point] += 4;
  expected_access_counters[1][SegmentAccessCounter::AccessType::Point] += 4;
  expected_access_counters[2][SegmentAccessCounter::AccessType::Point] += 4;
  verify_access_counters(__LINE__);

  ChunkEncoder::encode_all_chunks(input_table, GetParam());

  // The access counters were reset during encoding
  expected_access_counters = std::vector<SegmentAccessCounter>(3);

  // For all but dictionary encoding, generate_chunk_pruning_statistics iterated over the data to create the filters
  const auto is_dictionary = GetParam().encoding_type == EncodingType::Dictionary ||
                             GetParam().encoding_type == EncodingType::FixedStringDictionary;
  if (!is_dictionary) {
    expected_access_counters[0][SegmentAccessCounter::AccessType::Sequential] += 4;
    expected_access_counters[1][SegmentAccessCounter::AccessType::Sequential] += 4;
    expected_access_counters[2][SegmentAccessCounter::AccessType::Sequential] += 4;
  }
  verify_access_counters(__LINE__);

  const auto table_wrapper = std::make_shared<TableWrapper>(input_table);
  table_wrapper->execute();

  // Perform a scan
  auto scan_1 = create_table_scan(table_wrapper, ColumnID{2}, PredicateCondition::NotEquals, 10);
  scan_1->execute();

  // Expect the segment for column c to have been read sequentially
  expected_access_counters[2][SegmentAccessCounter::AccessType::Sequential] += 4;
  // For dictionary encoding, the dictionaries are accessed, too. The exact number of accesses depends on the
  // number of unique values and the implementation of the binary search.
  if (is_dictionary) {
    expected_access_counters[2][SegmentAccessCounter::AccessType::Dictionary] += 4;
  }
  verify_access_counters(__LINE__);

  // Perform a scan
  auto scan_2 = create_table_scan(scan_1, ColumnID{0}, PredicateCondition::GreaterThan, 10);
  scan_2->execute();

  // Expect the segment for column a to have been read sequentially with a gap caused by the first scan
  expected_access_counters[0][SegmentAccessCounter::AccessType::Monotonic] += 3;
  if (is_dictionary) {
    expected_access_counters[0][SegmentAccessCounter::AccessType::Dictionary] += 2;
  }
  verify_access_counters(__LINE__);

  EXPECT_EQ(scan_2->get_output()->row_count(), 1);

  // Perform a scan
  auto scan_3 = create_table_scan(scan_2, ColumnID{0}, PredicateCondition::Equals, 11);
  scan_3->execute();

  expected_access_counters[0][SegmentAccessCounter::AccessType::Point] += 1;
  if (is_dictionary) {
    expected_access_counters[0][SegmentAccessCounter::AccessType::Dictionary] += 4;
  }
  verify_access_counters(__LINE__);

  EXPECT_EQ(scan_3->get_output()->row_count(), 1);
}

TEST_F(EncodedSegmentTest, SegmentReencoding) {
  // Use the row_count used for frame of reference segments.
  auto value_segment = _create_int_with_null_value_segment(_row_count(EncodingType::FrameOfReference));

  auto encoded_segment =
      this->_encode_segment(value_segment, DataType::Int,
                            SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_EQ(get_segment_encoding_spec(encoded_segment),
            (SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned}));
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
  encoded_segment = this->_encode_segment(value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::RunLength});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
  encoded_segment =
      this->_encode_segment(value_segment, DataType::Int,
                            SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128});
  EXPECT_EQ(get_segment_encoding_spec(encoded_segment),
            (SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128}));
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
  encoded_segment =
      this->_encode_segment(value_segment, DataType::Int,
                            SegmentEncodingSpec{EncodingType::LZ4, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
  encoded_segment = this->_encode_segment(
      value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
  encoded_segment = this->_encode_segment(
      value_segment, DataType::Int,
      SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
  encoded_segment = this->_encode_segment(value_segment, DataType::Int,
                                          SegmentEncodingSpec{EncodingType::LZ4, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
}

// Testing the internal data structures of Run Length-encoded segments for monotonically increasing values
TEST_F(EncodedSegmentTest, RunLengthEncodingMonotonicallyIncreasing) {
  constexpr auto row_count = int32_t{100};
  auto values = pmr_vector<int32_t>(row_count);

  for (auto row_id = int32_t{0u}; row_id < row_count; ++row_id) {
    values[row_id] = row_id;
  }

  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values));
  const auto encoded_segment =
      this->_encode_segment(value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::RunLength});

  const auto run_length_segment = std::dynamic_pointer_cast<const RunLengthSegment<int32_t>>(encoded_segment);
  ASSERT_TRUE(run_length_segment);

  EXPECT_EQ(run_length_segment->values()->size(), row_count);
  EXPECT_EQ(run_length_segment->end_positions()->size(), row_count);
  EXPECT_EQ(run_length_segment->end_positions()->front(), 0ul);
  EXPECT_EQ(run_length_segment->end_positions()->back(), 99ul);
}

TEST_F(EncodedSegmentTest, RunLengthEncodingVaryingRuns) {
  constexpr auto row_count = int32_t{100};
  constexpr auto single_value_runs_decreasing = 10;
  constexpr auto long_value_runs = 8;
  constexpr auto long_value_element_repititions = 10;
  constexpr auto single_value_runs_increasing =
      row_count - single_value_runs_decreasing - long_value_runs * long_value_element_repititions;
  auto values = pmr_vector<int32_t>(row_count);

  // fill first ten values with decreasing values
  for (auto row_id = int32_t{0u}; row_id < single_value_runs_decreasing; ++row_id) {
    values[row_id] = single_value_runs_decreasing - row_id;
  }

  // the next 8 values are repeated 10 times each (i.e., 80 values)
  auto row_id = single_value_runs_decreasing;
  for (auto value = int32_t{0}; value < long_value_runs; ++value) {
    for (auto repetition = 0; repetition < long_value_element_repititions; ++repetition) {
      values[row_id] = value;
      ++row_id;
    }
  }

  // fill remainder with increasing values
  while (row_id < row_count) {
    values[row_id] = row_id;
    ++row_id;
  }

  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values));
  const auto encoded_segment =
      this->_encode_segment(value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::RunLength});

  const auto run_length_segment = std::dynamic_pointer_cast<const RunLengthSegment<int32_t>>(encoded_segment);
  ASSERT_TRUE(run_length_segment);

  // values, end_positions, and null_values should have the same length
  EXPECT_EQ(run_length_segment->values()->size(), run_length_segment->end_positions()->size());
  EXPECT_EQ(run_length_segment->values()->size(), run_length_segment->null_values()->size());
  EXPECT_EQ(run_length_segment->values()->size(),
            single_value_runs_decreasing + long_value_runs + single_value_runs_increasing);

  EXPECT_EQ(run_length_segment->end_positions()->front(), 0ul);
  EXPECT_EQ(run_length_segment->end_positions()->back(), 99ul);

  // first longer run has the value 10, starts at position 10, and ends at position 19
  EXPECT_EQ(run_length_segment->values()->at(10), 0);
  EXPECT_EQ(run_length_segment->end_positions()->at(9), 9);
  EXPECT_EQ(run_length_segment->end_positions()->at(10), 19);
  EXPECT_EQ(run_length_segment->values()->at(11), 1);

  // Values as expected
  EXPECT_EQ(run_length_segment->values()->at(0), 10);
  EXPECT_EQ(run_length_segment->values()->at(1), 9);
  EXPECT_EQ(run_length_segment->values()->at(9), 1);
  EXPECT_EQ(run_length_segment->values()->at(17), 7);
  EXPECT_EQ(run_length_segment->values()->at(18), 90);
  EXPECT_EQ(run_length_segment->values()->at(27), 99);
}

// Testing the internal data structures of Run Length-encoded segments for runs and NULL values where NULL values are
// fully covering single- and multi-element value runs.
TEST_F(EncodedSegmentTest, RunLengthEncodingNullValues) {
  constexpr auto row_count = int32_t{100};
  constexpr auto long_runs = 9;
  constexpr auto elements_per_long_run = 10;
  constexpr auto short_runs = 10;
  auto values = pmr_vector<int32_t>(row_count);
  auto null_values = pmr_vector<bool>(row_count);

  // the next 9 values are repeated 10 times each (i.e., 90 values)
  auto row_id = 0;
  for (auto value = int32_t{0}; value < long_runs; ++value) {
    for (auto repetition = 0; repetition < elements_per_long_run; ++repetition) {
      values[row_id] = value;

      // We set two values (i.e., two runs of 10 each) as being NULL
      if (value == 3 || value == 4) {
        null_values[row_id] = true;
      }

      ++row_id;
    }
  }

  // fill remainder with increasing values
  while (row_id < row_count) {
    values[row_id] = row_id;
    ++row_id;
  }
  null_values[95] = true;
  null_values[96] = true;

  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
  const auto encoded_segment =
      this->_encode_segment(value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::RunLength});

  const auto run_length_segment = std::dynamic_pointer_cast<const RunLengthSegment<int32_t>>(encoded_segment);
  ASSERT_TRUE(run_length_segment);

  // The handling of NULL values is unintuitive. When two runs of values are both NULL, they are merged into
  // a single run, only keeping the first value of both runs in values() as a placeholder for the merged NULL run.
  // That means the runs of values 3/4 and the single-value runs of 95/96 are stored as two value runs 3 and 95 in the
  // values() vector (thus, we substract two from the number of expected runs).
  EXPECT_EQ(run_length_segment->values()->size(), run_length_segment->end_positions()->size());
  EXPECT_EQ(run_length_segment->values()->size(), run_length_segment->null_values()->size());
  EXPECT_EQ(run_length_segment->values()->size(), long_runs + short_runs - 2);
  EXPECT_EQ(run_length_segment->end_positions()->front(), 9ul);
  EXPECT_EQ(run_length_segment->end_positions()->back(), 99ul);

  // Values runs for values 4 and 96 have been basically removed.
  EXPECT_EQ(run_length_segment->values()->at(3), 3);
  EXPECT_EQ(run_length_segment->values()->at(4), 5);
  EXPECT_EQ(run_length_segment->values()->at(13), 95);  // 9 + 5 - 1 (succeeding run of 4 removed from values())
  EXPECT_EQ(run_length_segment->values()->at(14), 97);  // no value 96 in values()

  // Check that successive NULL runs are merged to single position
  EXPECT_EQ(run_length_segment->null_values()->at(2), false);
  EXPECT_EQ(run_length_segment->null_values()->at(3), true);  // NULLs of values 3/4 are merged into single run
  EXPECT_EQ(run_length_segment->null_values()->at(4), false);
  EXPECT_EQ(run_length_segment->null_values()->at(12), false);
  EXPECT_EQ(run_length_segment->null_values()->at(13), true);
  EXPECT_EQ(run_length_segment->null_values()->at(14), false);
}

// Testing the internal data structures of Run Length-encoded segments for runs and NULL values where NULL values are
// either at the very first or last position, or a NULL value run spans two value runs.
TEST_F(EncodedSegmentTest, RunLengthEncodingNullValuesInRun) {
  constexpr auto row_count = int32_t{20};
  constexpr auto run_count = 2;
  constexpr auto value_repititions = 10;
  auto values = pmr_vector<int32_t>(row_count);
  auto null_values = pmr_vector<bool>(row_count);

  // two value runs, 10 each
  auto row_id = size_t{0};
  for (auto value = int32_t{0u}; value < run_count; ++value) {
    for (auto index = int32_t{0u}; index < value_repititions; ++index) {
      values[row_id] = value;
      ++row_id;
    }
  }

  // Adding four NULL value runs (three single element runs, one two element run)
  null_values[0] = true;  // very first position is NULL
  null_values[7] = true;  // single value in run is NULL

  // range of NULLs over position where values/run change
  null_values[9] = true;
  null_values[10] = true;

  null_values[19] = true;  // last position is NULL

  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
  const auto encoded_segment =
      this->_encode_segment(value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::RunLength});

  const auto run_length_segment = std::dynamic_pointer_cast<const RunLengthSegment<int32_t>>(encoded_segment);
  ASSERT_TRUE(run_length_segment);

  // NULL value runs are runs as well. We have four NULL runs and two value runs, where one run is split in two due to
  // a NULL value in between.
  EXPECT_EQ(run_length_segment->values()->size(), run_length_segment->end_positions()->size());
  EXPECT_EQ(run_length_segment->values()->size(), run_length_segment->null_values()->size());
  EXPECT_EQ(run_length_segment->values()->size(), 4 + run_count + 1);

  EXPECT_EQ(run_length_segment->end_positions()->front(), 0);  // value run longer, but first position is NULL
  EXPECT_EQ(run_length_segment->end_positions()->at(1), 6);
  EXPECT_EQ(run_length_segment->end_positions()->at(2), 7);
  EXPECT_EQ(run_length_segment->end_positions()->back(), 19ul);

  // Run is split as NULL value occur, hence values can repeat
  EXPECT_EQ(run_length_segment->values()->at(1), run_length_segment->values()->at(2));

  // NULL value run spans two elements of different value runs (last value of first run, first of second run). Hence,
  // second value run starts at position 11 instead of 10 due to NULL value.
  EXPECT_EQ(run_length_segment->end_positions()->at(3), 8);
  EXPECT_EQ(run_length_segment->end_positions()->at(4), 10);
}

// Testing the internal data structures of Frame of Reference-encoded segments. In particular, the determination of the
// reference value (i.e., the block minimum) as well as the difference to the reference value are checked. This test
// does not test the creation of multiple frames as the required vectors are too large for unit testing.
TEST_F(EncodedSegmentTest, FrameOfReference) {
  constexpr auto row_count = int32_t{17};
  constexpr auto minimum = int32_t{5};
  auto values = pmr_vector<int32_t>(row_count);
  auto null_values = pmr_vector<bool>(row_count);

  for (auto row_id = int32_t{0u}; row_id < row_count; ++row_id) {
    values[row_id] = minimum + row_id;
  }
  null_values[1] = true;
  null_values[7] = true;

  auto values_copy = values;
  auto null_values_copy = null_values;
  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
  const auto encoded_segment =
      this->_encode_segment(value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::FrameOfReference});

  const auto for_segment = std::dynamic_pointer_cast<const FrameOfReferenceSegment<int32_t>>(encoded_segment);
  ASSERT_TRUE(for_segment);

  EXPECT_EQ(for_segment->block_minima().size(), 1);  // single block
  EXPECT_EQ(for_segment->offset_values().size(), row_count);
  EXPECT_TRUE(for_segment->null_values());
  EXPECT_EQ(for_segment->null_values()->size(), row_count);

  EXPECT_EQ((*for_segment->null_values())[0], false);
  EXPECT_EQ((*for_segment->null_values())[1], true);
  EXPECT_EQ((*for_segment->null_values())[7], true);
  EXPECT_EQ((*for_segment->null_values())[16], false);

  // Block minium should be the smallest value: 0
  EXPECT_EQ(for_segment->block_minima().front(), minimum);
  resolve_compressed_vector_type(for_segment->offset_values(), [&](const auto& offset_values) {
    auto offset_iter = offset_values.cbegin();
    auto values_iter = values_copy.cbegin();
    auto null_values_iter = null_values_copy.cbegin();
    for (; offset_iter != offset_values.cend(); ++offset_iter, ++values_iter, ++null_values_iter) {
      if (!*null_values_iter) {
        EXPECT_EQ(*offset_iter, *values_iter - minimum);
      }
    }
  });

  // Check that NULLs are not stored for FoR segment that does not contain any NULLs
  const auto value_segment_no_nulls = std::make_shared<ValueSegment<int32_t>>(std::move(values_copy));
  const auto encoded_segment_no_nulls =
      this->_encode_segment(value_segment_no_nulls, DataType::Int, SegmentEncodingSpec{EncodingType::FrameOfReference});

  const auto for_segment_no_nulls =
      std::dynamic_pointer_cast<const FrameOfReferenceSegment<int32_t>>(encoded_segment_no_nulls);
  ASSERT_TRUE(for_segment_no_nulls);
  EXPECT_FALSE(for_segment_no_nulls->null_values());
}

}  // namespace opossum
