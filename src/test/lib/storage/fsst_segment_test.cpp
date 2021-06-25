#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/fsst_segment.hpp"
#include "storage/fsst_segment/fsst_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

#include <iostream>

namespace opossum {

// A simple polymorphic memory resource that tracks how much memory was allocated
class SimpleTrackingMemoryResource : public boost::container::pmr::memory_resource {
 public:
  size_t allocated{0};

  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    allocated += bytes;
    return std::malloc(bytes);  // NOLINT
  }

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
    allocated -= bytes;
    std::free(p);  // NOLINT
  }

  bool do_is_equal(const memory_resource& other) const noexcept override { Fail("Not implemented"); }
};

class StorageFSSTSegmentTest : public BaseTest {
 protected:
  //  static constexpr auto row_count = FSSTEncoder::_block_size + size_t{1000u};
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>(true);
};

template <typename T>
std::shared_ptr<FSSTSegment<T>> compress(std::shared_ptr<ValueSegment<T>> segment, DataType data_type) {
  auto encoded_segment = ChunkEncoder::encode_segment(segment, data_type, SegmentEncodingSpec{EncodingType::FSST});
  return std::dynamic_pointer_cast<FSSTSegment<T>>(encoded_segment);
}

TEST_F(StorageFSSTSegmentTest, CreateEmptyFSSTSegmentTest) {
  pmr_vector<pmr_string> values;
  pmr_vector<bool> null_values;
  FSSTSegment<pmr_string> segment(values, null_values);

  ASSERT_EQ(segment.size(), 0);
  //  ASSERT_EQ(segment.memory_usage(MemoryUsageCalculationMode::Full), 0); TODO(anyone): check memory consumption
}

TEST_F(StorageFSSTSegmentTest, MemoryUsageSegmentTest) {
  vs_str->append("Moritz");
  vs_str->append("ChrisChr");
  vs_str->append("Christopher");
  auto segment = compress(vs_str, DataType::String);

  ASSERT_EQ(segment->size(), 3);
  //  ASSERT_EQ(segment.memory_usage(MemoryUsageCalculationMode::Full), 12); TODO(anyone): check memory consumption
}

TEST_F(StorageFSSTSegmentTest, CreateFSSTSegmentTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab"};
  pmr_vector<bool> null_values = {false};
  FSSTSegment<pmr_string> segment(values, null_values);
}

TEST_F(StorageFSSTSegmentTest, DecompressFSSTSegmentTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab"};
  FSSTSegment<pmr_string> segment(values, std::nullopt);

  std::optional<pmr_string> value = segment.get_typed_value(ChunkOffset{2});
  ASSERT_EQ(values[2], value.value());
}

TEST_F(StorageFSSTSegmentTest, CopyUsingAllocatorFSSTSegmentTest) {
  pmr_vector<pmr_string> values = {"Moritz", "Chris", "Peter"};

  for (const auto& value : values) {
    vs_str->append(value);
  }

  auto resource = SimpleTrackingMemoryResource{};
  const auto allocator = PolymorphicAllocator<size_t>(&resource);

  auto segment = compress(vs_str, DataType::String);
  auto copied_segment = std::dynamic_pointer_cast<FSSTSegment<pmr_string>>(segment->copy_using_allocator(allocator));

  ASSERT_EQ(copied_segment->size(), values.size());
  for (size_t index = 0; index < values.size(); ++index) {
    ASSERT_EQ(copied_segment->get_typed_value(index), std::optional{values[index]});
  }
}

TEST_F(StorageFSSTSegmentTest, DecompressNullFSSTSegmentTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", ""};
  pmr_vector<bool> null_values = {false, false, true};
  FSSTSegment<pmr_string> segment(values, std::optional(null_values));

  std::optional<pmr_string> value = segment.get_typed_value(ChunkOffset{2});
  ASSERT_EQ(std::nullopt, value);
}

TEST_F(StorageFSSTSegmentTest, FSSTSegmentIterableTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", ""};
  pmr_vector<bool> null_values = {false, false, true};
  FSSTSegment<pmr_string> segment(values, std::optional(null_values));

  pmr_vector<SegmentPosition<pmr_string>> collected_values;

  auto segment_iterable = FSSTSegmentIterable(segment);

  segment_iterable._on_with_iterators([&collected_values](auto it, auto end) {
    while (it != end) {
      collected_values.push_back(*it);
      it++;
    }
  });

  for (size_t index = 0; index < values.size(); ++index) {
    auto segment_position = collected_values.at(index);
    ASSERT_EQ(segment_position.is_null(), null_values.at(index));
    if (!null_values.at(index)) {
      ASSERT_EQ(segment_position.value(), values.at(index));
    }
  }
}

TEST_F(StorageFSSTSegmentTest, FSSTSegmentPointIterableTest) {
  pmr_vector<pmr_string> values{"Moritz", "ChrisChr", ""};
  pmr_vector<bool> null_values = {false, false, true};
  FSSTSegment<pmr_string> segment(values, std::optional(null_values));

  pmr_vector<SegmentPosition<pmr_string>> collected_values;

  auto segment_iterable = FSSTSegmentIterable(segment);

  const auto position_filter = std::make_shared<RowIDPosList>();
  position_filter->emplace_back(RowID{ChunkID{0}, ChunkOffset{1}});
  position_filter->emplace_back(RowID{ChunkID{0}, ChunkOffset{2}});
  position_filter->guarantee_single_chunk();

  segment_iterable.with_iterators(position_filter, [&collected_values](auto it, auto end) {
    while (it != end) {
      collected_values.push_back(*it);
      it++;
    }
  });

  for (size_t index = 0; index < position_filter->size(); ++index) {
    auto position = (*position_filter)[index];
    auto real_chunk_offset = position.chunk_offset;
    auto segment_position = collected_values.at(index);

    ASSERT_EQ(segment_position.is_null(), null_values.at(real_chunk_offset));
    if (!null_values.at(real_chunk_offset)) {
      ASSERT_EQ(segment_position.value(), values.at(real_chunk_offset));
    }
  }
}

}  // namespace opossum
