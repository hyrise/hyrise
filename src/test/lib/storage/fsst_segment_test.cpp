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

namespace opossum {

// A simple polymorphic memory resource that tracks how much memory was allocated
class FSSTSimpleTrackingMemoryResource : public boost::container::pmr::memory_resource {
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
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>(true);
};

template <typename T>
std::shared_ptr<FSSTSegment<T>> create_fsst_segment(std::shared_ptr<ValueSegment<T>> segment, DataType data_type) {
  auto encoded_segment = ChunkEncoder::encode_segment(segment, data_type, SegmentEncodingSpec{EncodingType::FSST});
  return std::dynamic_pointer_cast<FSSTSegment<T>>(encoded_segment);
}

TEST_F(StorageFSSTSegmentTest, CreateEmptyFSSTSegmentTest) {
  auto segment = create_fsst_segment(vs_str, DataType::String);

  ASSERT_EQ(segment->size(), 0);
  ASSERT_EQ(segment->memory_usage(MemoryUsageCalculationMode::Full), 2576);
}

TEST_F(StorageFSSTSegmentTest, MemoryUsageSegmentTest) {
  vs_str->append("This");
  vs_str->append("is");
  vs_str->append("data");
  auto segment = create_fsst_segment(vs_str, DataType::String);

  ASSERT_EQ(segment->size(), 3);
  // DECODER_SIZE = 8*256 + 256 + 1 + 8 = 2`313
  ASSERT_EQ(segment->memory_usage(MemoryUsageCalculationMode::Full), 2594);
}

TEST_F(StorageFSSTSegmentTest, DecompressFSSTSegmentTest) {
  const auto values = pmr_vector<pmr_string>{"This", "is", "a", "test", "for", "the", "FSST", "segment"};
  for (const auto& value : values) {
    vs_str->append(value);
  }
  const auto segment = create_fsst_segment(vs_str, DataType::String);

  for (auto index = 0UL; index < values.size(); ++index) {
    std::optional<pmr_string> value = segment->get_typed_value(static_cast<ChunkOffset>(index));
    ASSERT_EQ(*value, values[index]);
  }
}

TEST_F(StorageFSSTSegmentTest, CopyUsingAllocatorFSSTSegmentTest) {
  const auto values = pmr_vector<pmr_string>{"This", "is", "data"};

  for (const auto& value : values) {
    vs_str->append(value);
  }

  auto resource = FSSTSimpleTrackingMemoryResource{};
  const auto allocator = PolymorphicAllocator<size_t>(&resource);

  const auto segment = create_fsst_segment(vs_str, DataType::String);
  const auto copied_segment =
      std::dynamic_pointer_cast<FSSTSegment<pmr_string>>(segment->copy_using_allocator(allocator));

  ASSERT_EQ(copied_segment->size(), values.size());
  for (size_t index = 0; index < values.size(); ++index) {
    ASSERT_EQ(std::optional{values[index]}, copied_segment->get_typed_value(index));
  }
}

TEST_F(StorageFSSTSegmentTest, DecompressNullFSSTSegmentTest) {
  vs_str->append("This");
  vs_str->append("is");
  vs_str->append(NULL_VALUE);
  const auto segment = create_fsst_segment(vs_str, DataType::String);

  std::optional<pmr_string> value = segment->get_typed_value(ChunkOffset{2});
  ASSERT_EQ(std::nullopt, value);
}

TEST_F(StorageFSSTSegmentTest, FSSTSegmentIterableTest) {
  const auto values = pmr_vector<pmr_string>{"This", "is", "data"};
  const auto expected_null_values = pmr_vector<bool>{false, false, true};

  for (auto index = 0UL; index < values.size(); ++index) {
    vs_str->append((expected_null_values[index]) ? NULL_VALUE : values[index]);
  }

  const auto segment = create_fsst_segment(vs_str, DataType::String);
  pmr_vector<SegmentPosition<pmr_string>> collected_values;

  auto segment_iterable = FSSTSegmentIterable(*segment);

  segment_iterable._on_with_iterators([&collected_values](auto it, auto end) {
    while (it != end) {
      collected_values.push_back(*it);
      it++;
    }
  });

  for (auto index = 0UL; index < values.size(); ++index) {
    auto segment_position = collected_values.at(index);
    ASSERT_EQ(expected_null_values.at(index), segment_position.is_null());
    if (!expected_null_values.at(index)) {
      ASSERT_EQ(values.at(index), segment_position.value());
    }
  }
}

TEST_F(StorageFSSTSegmentTest, FSSTSegmentPointIterableTest) {
  const auto values = pmr_vector<pmr_string>{"Two", "values", ""};
  const auto expected_null_values = pmr_vector<bool>{false, false, true};

  for (auto index = 0UL; index < values.size(); ++index) {
    vs_str->append((expected_null_values[index]) ? NULL_VALUE : values[index]);
  }

  const auto segment = create_fsst_segment(vs_str, DataType::String);
  pmr_vector<SegmentPosition<pmr_string>> collected_values;

  auto segment_iterable = FSSTSegmentIterable(*segment);

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

  for (auto index = 0UL; index < position_filter->size(); ++index) {
    const auto position = (*position_filter)[index];
    const auto real_chunk_offset = position.chunk_offset;
    const auto segment_position = collected_values.at(index);

    ASSERT_EQ(expected_null_values.at(real_chunk_offset), segment_position.is_null());
    if (!expected_null_values.at(real_chunk_offset)) {
      ASSERT_EQ(values.at(real_chunk_offset), segment_position.value());
    }
  }
}

}  // namespace opossum
