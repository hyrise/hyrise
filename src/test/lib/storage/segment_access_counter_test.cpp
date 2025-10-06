#include <gtest/gtest.h>

#include <cstddef>
#include <memory>

#include "magic_enum/magic_enum.hpp"

#include "base_test.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/segment_access_counter.hpp"
#include "types.hpp"

namespace hyrise {

class SegmentAccessCounterTest : public BaseTest {
 protected:
  using AccessPattern = SegmentAccessCounter::AccessPattern;
  using AccessType = SegmentAccessCounter::AccessType;

  static AccessPattern _access_pattern(const std::shared_ptr<const RowIDPosList>& positions) {
    return SegmentAccessCounter::_access_pattern(*positions);
  }
};

TEST_F(SegmentAccessCounterTest, ZeroOnConstruction) {
  auto counter = SegmentAccessCounter{};
  for (auto access_type = size_t{0}; access_type < magic_enum::enum_count<AccessType>(); ++access_type) {
    EXPECT_EQ(0, counter[static_cast<AccessType>(access_type)]);
  }
}

TEST_F(SegmentAccessCounterTest, ToString) {
  auto counter = SegmentAccessCounter{};
  counter[AccessType::Point] = 1;
  counter[AccessType::Sequential] = 20;
  counter[AccessType::Monotonic] = 300;
  counter[AccessType::Random] = 4'000;
  counter[AccessType::Dictionary] = 50'000;

  const auto expected_str = "1,20,300,4000,50000";
  EXPECT_EQ(expected_str, counter.to_string());
}

TEST_F(SegmentAccessCounterTest, CopyConstructor) {
  auto counter1 = SegmentAccessCounter{};
  counter1[AccessType::Point] = 1;
  counter1[AccessType::Sequential] = 20;
  counter1[AccessType::Monotonic] = 300;
  counter1[AccessType::Random] = 4'000;
  counter1[AccessType::Dictionary] = 50'000;

  SegmentAccessCounter counter2{counter1};
  for (auto access_type = size_t{0}; access_type < magic_enum::enum_count<AccessType>(); ++access_type) {
    EXPECT_EQ(counter1[static_cast<AccessType>(access_type)], counter2[static_cast<AccessType>(access_type)]);
  }
}

TEST_F(SegmentAccessCounterTest, AssignmentOperator) {
  auto counter1 = SegmentAccessCounter{};
  counter1[AccessType::Point] = 1;
  counter1[AccessType::Sequential] = 20;
  counter1[AccessType::Monotonic] = 300;
  counter1[AccessType::Random] = 4'000;
  counter1[AccessType::Dictionary] = 50'000;

  auto counter2 = SegmentAccessCounter{};
  counter2 = counter1;
  for (auto access_type = size_t{0}; access_type < magic_enum::enum_count<AccessType>(); ++access_type) {
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

}  // namespace hyrise
