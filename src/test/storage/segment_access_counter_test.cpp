#include "gtest/gtest.h"

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "base_test.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_access_counter.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class SegmentAccessCounterCounterTest : public BaseTest {
 public:
  static void SetUpTestCase() {}

  void SetUp() override {}

  void TearDown() override {}
};

class SegmentAccessCounterTest : public BaseTest {
 public:
  static void SetUpTestCase() {}

  void SetUp() override {}

  void TearDown() override {}

 protected:
  using AccessPattern = SegmentAccessCounter::AccessPattern;
  using Input = SegmentAccessCounter::Input;

  static AccessPattern _iterator_access_pattern(const std::shared_ptr<const PosList>& positions) {
    return SegmentAccessCounter::_iterator_access_pattern(positions);
  }
};

typedef ::testing::Types<uint64_t, std::atomic_uint64_t> CounterTypes;
TYPED_TEST_SUITE(SegmentAccessCounterCounterTest, CounterTypes);

TYPED_TEST(SegmentAccessCounterCounterTest, ZeroOnConstruction) {
  SegmentAccessCounter::Counter<TypeParam> counter;
  EXPECT_EQ(0, counter.sum());
}

TYPED_TEST(SegmentAccessCounterCounterTest, Sum36) {
  SegmentAccessCounter::Counter<TypeParam> counter;
  EXPECT_EQ(0, counter.sum());
  counter.other = 1;
  counter.iterator_create = 2;
  counter.iterator_seq_access = 3;
  counter.iterator_increasing_access = 4;
  counter.iterator_random_access = 5;
  counter.accessor_create = 6;
  counter.accessor_access = 7;
  counter.dictionary_access = 8;
  EXPECT_EQ(36, counter.sum());
}

TYPED_TEST(SegmentAccessCounterCounterTest, ToString) {
  SegmentAccessCounter::Counter<TypeParam> counter;
  counter.other = 1;
  counter.iterator_create = 20;
  counter.iterator_seq_access = 300;
  counter.iterator_increasing_access = 4'000;
  counter.iterator_random_access = 50'000;
  counter.accessor_create = 600'000;
  counter.accessor_access = 7'000'000;
  counter.dictionary_access = 80'000'000;
  const auto expected_str = "1,20,300,4000,50000,600000,7000000,80000000";
  EXPECT_EQ(expected_str, counter.to_string());
}

TYPED_TEST(SegmentAccessCounterCounterTest, Reset) {
  SegmentAccessCounter::Counter<TypeParam> counter;
  counter.other = 1;
  counter.iterator_create = 20;
  counter.iterator_seq_access = 300;
  counter.iterator_increasing_access = 4'000;
  counter.iterator_random_access = 50'000;
  counter.accessor_create = 600'000;
  counter.accessor_access = 7'000'000;
  counter.dictionary_access = 80'000'000;
  EXPECT_EQ(87'654'321, counter.sum());
  counter.reset();
  EXPECT_EQ(0, counter.sum());
}

TEST_F(SegmentAccessCounterTest, CounterReset) {
  ValueSegment<int32_t> vs{false};
  vs.append(42);
  vs.append(66);
  vs.append(666);
  EXPECT_EQ(3, vs.access_counter.counter().sum());
  vs.access_counter.reset();
  EXPECT_EQ(0, vs.access_counter.counter().sum());
}

TEST_F(SegmentAccessCounterTest, IteratorAccessPatternIncreasing) {
  auto positions = std::make_shared<PosList>();
  EXPECT_EQ(AccessPattern::Unknown, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{0}});
  EXPECT_EQ(AccessPattern::Unknown, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{0}});
  EXPECT_EQ(AccessPattern::Unknown, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{1}});
  EXPECT_EQ(AccessPattern::SeqInc, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(AccessPattern::RndInc, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(AccessPattern::RndInc, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{1}});
  EXPECT_EQ(AccessPattern::Rnd, _iterator_access_pattern(positions));
}
TEST_F(SegmentAccessCounterTest, IteratorAccessPatternDecreasing) {
  auto positions = std::make_shared<PosList>();
  EXPECT_EQ(AccessPattern::Unknown, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(AccessPattern::Unknown, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(AccessPattern::Unknown, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(AccessPattern::SeqDec, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(AccessPattern::SeqDec, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{660}});
  EXPECT_EQ(AccessPattern::RndDec, _iterator_access_pattern(positions));
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(AccessPattern::Rnd, _iterator_access_pattern(positions));
}

}  // namespace opossum