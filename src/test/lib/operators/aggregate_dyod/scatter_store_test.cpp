#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "base_test.hpp"

#include "operators/aggregate_dyod/scatter_store.hpp"

namespace hyrise {
class RegionTest : public BaseTest {
protected:
  static std::array<std::byte, SWWC_LINE_BYTES> make_line(const uint8_t seed) {
    auto line = std::array<std::byte, SWWC_LINE_BYTES>{};
    for (auto index = size_t{0}; index < SWWC_LINE_BYTES; ++index) {
      line[index] = static_cast<std::byte>(static_cast<uint8_t>(seed + index));
    }
    return line;
  }

  static std::vector<std::byte> make_bytes(const size_t length, const uint8_t seed) {
    auto bytes = std::vector<std::byte>(length);
    for (auto index = size_t{0}; index < length; ++index) {
      bytes[index] = static_cast<std::byte>(static_cast<uint8_t>(seed + index));
    }
    return bytes;
  }

  static void expect_region_bytes(const Region& region, const std::vector<std::byte>& expected) {
    ASSERT_EQ(region.size(), expected.size());
    if (!expected.empty()) {
      EXPECT_EQ(std::memcmp(region.data(), expected.data(), expected.size()), 0);
    }
  }

  static std::vector<std::byte> concat(const std::vector<std::array<std::byte, SWWC_LINE_BYTES>>& lines) {
    auto bytes = std::vector<std::byte>{};
    bytes.reserve(lines.size() * SWWC_LINE_BYTES);
    for (const auto& line : lines) {
      bytes.insert(bytes.end(), line.begin(), line.end());
    }
    return bytes;
  }

  Region _region{};
};

TEST_F(RegionTest, PushSingleLineStoresBytesAndAdvancesSize) {
  const auto line = make_line(0x10);

  _region.push_line(line.data());

  EXPECT_EQ(_region.size(), SWWC_LINE_BYTES);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(_region.data()) % 64, 0u);
  expect_region_bytes(_region, std::vector(line.begin(), line.end()));
}


TEST_F(RegionTest, PushMultipleLinesAccumulateInOrder) {
  auto lines = std::vector<std::array<std::byte, SWWC_LINE_BYTES>>{};

  for (auto i = size_t{0}; i < 5; ++i) {
    const auto line = make_line(static_cast<uint8_t>(i));
    lines.push_back(line);
    _region.push_line(line.data());
  }

  EXPECT_EQ(_region.size(), SWWC_LINE_BYTES * 5);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(_region.data()) % 64, 0u);
  expect_region_bytes(_region, concat(lines));
}

TEST_F(RegionTest, DrainPartialAppendsSubLineRemainder) {
  constexpr auto partial_length = SWWC_LINE_BYTES / 2;
  const auto partial = make_bytes(partial_length, 0x30);

  _region.drain_partial(partial.data(), partial_length);

  EXPECT_EQ(_region.size(), partial_length);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(_region.data()) % 64, 0u);
  expect_region_bytes(_region, partial);
}

TEST_F(RegionTest, DrainPartialZeroLengthIsNoop) {
  _region.drain_partial(nullptr, 0);
  EXPECT_EQ(_region.size(), 0u);

  const auto line = make_line(0x20);
  _region.push_line(line.data());

  _region.drain_partial(nullptr, 0);
  EXPECT_EQ(_region.size(), SWWC_LINE_BYTES);
  expect_region_bytes(_region, std::vector(line.begin(), line.end()));
}

TEST_F(RegionTest, PushLinesThenDrainPartial) {
  auto lines = std::vector<std::array<std::byte, SWWC_LINE_BYTES>>{};
  for (auto i = size_t{0}; i < 3; ++i) {
    const auto line = make_line(static_cast<uint8_t>(0x40 + i));
    lines.push_back(line);
    _region.push_line(line.data());
  }

  constexpr auto partial_length = SWWC_LINE_BYTES / 2;
  const auto partial = make_bytes(partial_length, 0x80);
  _region.drain_partial(partial.data(), partial_length);

  EXPECT_EQ(_region.size(), SWWC_LINE_BYTES * 3 + partial_length);

  auto expected = concat(lines);
  expected.insert(expected.end(), partial.begin(), partial.end());
  expect_region_bytes(_region, expected);
}

TEST_F(RegionTest, GrowthPreservesContentsAndAlignment) {
  constexpr auto LINE_COUNT = size_t{256};
  auto lines = std::vector<std::array<std::byte, SWWC_LINE_BYTES>>{};
  lines.reserve(LINE_COUNT);
  for (auto i = size_t{0}; i < LINE_COUNT; ++i) {
    const auto line = make_line(static_cast<uint8_t>(i));
    lines.push_back(line);
    _region.push_line(line.data());
  }

  EXPECT_EQ(_region.size(), SWWC_LINE_BYTES * LINE_COUNT);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(_region.data()) % 64, 0u);
  expect_region_bytes(_region, concat(lines));
}

TEST_F(RegionTest, ClearResetsSizeAndRetainsCapacity) {
  for (auto i = size_t{0}; i < 5; ++i) {
    const auto line = make_line(static_cast<uint8_t>(i));
    _region.push_line(line.data());
  }
  const auto* const buffer_before = _region.data();

  _region.clear();
  EXPECT_EQ(_region.size(), 0u);

  auto lines = std::vector<std::array<std::byte, SWWC_LINE_BYTES>>{};
  for (auto i = size_t{0}; i < 5; ++i) {
    const auto line = make_line(static_cast<uint8_t>(0x90 + i));
    lines.push_back(line);
    _region.push_line(line.data());
  }

  EXPECT_EQ(_region.data(), buffer_before);
  EXPECT_EQ(_region.size(), SWWC_LINE_BYTES * 5);
  expect_region_bytes(_region, concat(lines));
}

TEST_F(RegionTest, EmptyRegionHasZeroSize) {
  EXPECT_EQ(_region.size(), 0);
  _region.clear();
  EXPECT_EQ(_region.size(), 0u);
}
}