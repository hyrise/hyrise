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

class SegmentAccessCounterTest : public BaseTest {
 protected:
  using AccessPattern = SegmentAccessCounter::AccessPattern;
  using AccessType = SegmentAccessCounter::AccessType;

  static AccessPattern _access_pattern(const std::shared_ptr<const RowIDPosList>& positions) {
    return SegmentAccessCounter::_access_pattern(*positions);
  }
};

TEST_F(SegmentAccessCounterTest, ZeroOnConstruction) {
  SegmentAccessCounter counter;
  for (auto access_type = 0ul; access_type < static_cast<size_t>(AccessType::Count); ++access_type) {
    EXPECT_EQ(0, counter[static_cast<AccessType>(access_type)]);
  }
}

TEST_F(SegmentAccessCounterTest, ToString) {
  SegmentAccessCounter counter;
  counter[AccessType::Point] = 1;
  counter[AccessType::Sequential] = 20;
  counter[AccessType::Monotonic] = 300;
  counter[AccessType::Random] = 4'000;
  counter[AccessType::Dictionary] = 50'000;

  const auto expected_str = "1,20,300,4000,50000";
  EXPECT_EQ(expected_str, counter.to_string());
}

TEST_F(SegmentAccessCounterTest, CopyConstructor) {
  SegmentAccessCounter counter1;
  counter1[AccessType::Point] = 1;
  counter1[AccessType::Sequential] = 20;
  counter1[AccessType::Monotonic] = 300;
  counter1[AccessType::Random] = 4'000;
  counter1[AccessType::Dictionary] = 50'000;

  SegmentAccessCounter counter2{counter1};
  for (auto access_type = 0ul; access_type < static_cast<size_t>(AccessType::Count); ++access_type) {
    EXPECT_EQ(counter1[static_cast<AccessType>(access_type)], counter2[static_cast<AccessType>(access_type)]);
  }
}

TEST_F(SegmentAccessCounterTest, AssignmentOperator) {
  SegmentAccessCounter counter1;
  counter1[AccessType::Point] = 1;
  counter1[AccessType::Sequential] = 20;
  counter1[AccessType::Monotonic] = 300;
  counter1[AccessType::Random] = 4'000;
  counter1[AccessType::Dictionary] = 50'000;

  SegmentAccessCounter counter2;
  counter2 = counter1;
  for (auto access_type = 0ul; access_type < static_cast<size_t>(AccessType::Count); ++access_type) {
    EXPECT_EQ(counter1[static_cast<AccessType>(access_type)], counter2[static_cast<AccessType>(access_type)]);
  }
}

TEST_F(SegmentAccessCounterTest, AccessPattern1) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{0}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{0}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{1}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{1}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
  positions->push_back({ChunkID{0}, ChunkOffset{1}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
  positions->push_back({ChunkID{0}, ChunkOffset{2}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
  positions->push_back({ChunkID{0}, ChunkOffset{4}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
  positions->push_back({ChunkID{0}, ChunkOffset{0}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

TEST_F(SegmentAccessCounterTest, AccessPattern2) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{0}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{4}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{3}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

TEST_F(SegmentAccessCounterTest, AccessPattern3) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{660}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{659}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{659}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

TEST_F(SegmentAccessCounterTest, AccessPattern4) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{664}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{663}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{664}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

TEST_F(SegmentAccessCounterTest, AccessPattern5) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{664}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

TEST_F(SegmentAccessCounterTest, AccessPattern6) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{667}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

TEST_F(SegmentAccessCounterTest, AccessPattern7) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{666}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::SequentiallyIncreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{660}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

TEST_F(SegmentAccessCounterTest, AccessPattern8) {
  auto positions = std::make_shared<RowIDPosList>();
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{665}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Point);
  positions->push_back({ChunkID{0}, ChunkOffset{660}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::MonotonicallyDecreasing);
  positions->push_back({ChunkID{0}, ChunkOffset{661}});
  EXPECT_EQ(_access_pattern(positions), AccessPattern::Random);
}

}  // namespace opossum
