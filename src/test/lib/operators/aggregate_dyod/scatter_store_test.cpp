#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

#include "base_test.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/scatter_store.hpp"

namespace {
std::array<std::byte, hyrise::SWWC_LINE_BYTES> make_line(const uint8_t seed) {
  auto line = std::array<std::byte, hyrise::SWWC_LINE_BYTES>{};
  for (auto index = size_t{0}; index < hyrise::SWWC_LINE_BYTES; ++index) {
    line[index] = static_cast<std::byte>(static_cast<uint8_t>(seed + index));
  }
  return line;
}

std::vector<std::byte> make_bytes(const size_t length, const uint8_t seed) {
  auto bytes = std::vector<std::byte>(length);
  for (auto index = size_t{0}; index < length; ++index) {
    bytes[index] = static_cast<std::byte>(static_cast<uint8_t>(seed + index));
  }
  return bytes;
}

void expect_region_bytes(const hyrise::Region& region, const std::vector<std::byte>& expected) {
  ASSERT_EQ(region.size(), expected.size());
  if (!expected.empty()) {
    EXPECT_EQ(std::memcmp(region.data(), expected.data(), expected.size()), 0);
  }
}

std::vector<std::byte> concat(const std::vector<std::array<std::byte, hyrise::SWWC_LINE_BYTES>>& lines) {
  auto bytes = std::vector<std::byte>{};
  bytes.reserve(lines.size() * hyrise::SWWC_LINE_BYTES);
  for (const auto& line : lines) {
    bytes.insert(bytes.end(), line.begin(), line.end());
  }
  return bytes;
}
}  // namespace

namespace hyrise {
class RegionTest : public BaseTest {
 protected:
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

TEST_F(RegionTest, ReleaseFreesTheBuffer) {
  const auto line = make_line(0x20);
  _region.push_line(line.data());

  _region.release();

  EXPECT_EQ(_region.size(), 0u);
  EXPECT_EQ(_region.data(), nullptr);
}

TEST_F(RegionTest, ReleasedRegionAcceptsNewLines) {
  const auto line = make_line(0x30);
  _region.push_line(line.data());
  _region.release();

  _region.push_line(line.data());

  EXPECT_EQ(_region.size(), SWWC_LINE_BYTES);
  expect_region_bytes(_region, std::vector(line.begin(), line.end()));
}

TEST_F(RegionTest, EmptyRegionHasZeroSize) {
  EXPECT_EQ(_region.size(), 0);
  _region.clear();
  EXPECT_EQ(_region.size(), 0u);
}

class ScatterStoreTest : public BaseTest {};

TEST_F(ScatterStoreTest, ConstructionAllocatesRegionsForFullSchema) {
  constexpr auto value_widths = std::array<size_t, 2>{8, 4};
  auto store = ScatterStore(PartitionCount{4}, 8, std::span<const size_t>(value_widths), 1, true);

  for (auto partition = PartitionId{0}; partition < 4; ++partition) {
    EXPECT_EQ(store.key_region(partition).size(), 0u);
    EXPECT_EQ(store.value_region(partition, 0).size(), 0u);
    EXPECT_EQ(store.value_region(partition, 1).size(), 0u);
    EXPECT_EQ(store.value_null_bitmap_region(partition).size(), 0u);
  }
}

TEST_F(ScatterStoreTest, ClearResetsAllRegions) {
  constexpr auto value_widths = std::array<size_t, 1>{8};
  auto store = ScatterStore(PartitionCount{2}, 8, std::span<const size_t>(value_widths), 1, true);

  const auto line = make_line(0x50);
  for (auto partition = PartitionId{0}; partition < 2; ++partition) {
    store.key_region(partition).push_line(line.data());
    store.value_region(partition, 0).push_line(line.data());
    store.value_null_bitmap_region(partition).push_line(line.data());
  }

  store.clear();

  for (auto partition = PartitionId{0}; partition < 2; ++partition) {
    EXPECT_EQ(store.key_region(partition).size(), 0u);
    EXPECT_EQ(store.value_region(partition, 0).size(), 0u);
    EXPECT_EQ(store.value_null_bitmap_region(partition).size(), 0u);
  }
}

TEST_F(ScatterStoreTest, ReleaseFreesAllRegions) {
  constexpr auto value_widths = std::array<size_t, 1>{8};
  auto store = ScatterStore(PartitionCount{2}, 8, std::span<const size_t>(value_widths), 1, true);

  const auto line = make_line(0x60);
  for (auto partition = PartitionId{0}; partition < 2; ++partition) {
    store.key_region(partition).push_line(line.data());
    store.value_region(partition, 0).push_line(line.data());
    store.value_null_bitmap_region(partition).push_line(line.data());
  }

  store.release();

  for (auto partition = PartitionId{0}; partition < 2; ++partition) {
    EXPECT_EQ(store.key_region(partition).data(), nullptr);
    EXPECT_EQ(store.value_region(partition, 0).data(), nullptr);
    EXPECT_EQ(store.value_null_bitmap_region(partition).data(), nullptr);
  }
}

class ScatterHeadsTest : public BaseTest {};

TEST_F(ScatterHeadsTest, ScatterAndReconstruct) {
  constexpr auto PARTITION_COUNT = uint32_t{4};
  constexpr auto KEY_WIDTH = size_t{8};
  constexpr auto VALUE_WIDTH = size_t{8};
  static_assert(SWWC_LINE_BYTES % KEY_WIDTH == 0 && SWWC_LINE_BYTES % VALUE_WIDTH == 0);

  constexpr auto value_widths = std::array{VALUE_WIDTH};
  auto store =
      ScatterStore(PartitionCount{PARTITION_COUNT}, KEY_WIDTH, std::span<const size_t>(value_widths), 0, false);
  constexpr auto stream_widths = std::array{KEY_WIDTH, VALUE_WIDTH};
  auto heads = ScatterHeads(PartitionCount{PARTITION_COUNT}, 2, std::span<const size_t>(stream_widths), false);

  constexpr auto fields_per_line = SWWC_LINE_BYTES / KEY_WIDTH;
  constexpr auto rows_per_partition = 2 * fields_per_line + 3;
  constexpr auto row_count = size_t{PARTITION_COUNT} * rows_per_partition;

  auto expected_keys = std::vector<std::vector<std::byte>>(PARTITION_COUNT);
  auto expected_values = std::vector<std::vector<std::byte>>(PARTITION_COUNT);

  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto partition = static_cast<PartitionId>(row % PARTITION_COUNT);
    const auto key = make_bytes(KEY_WIDTH, static_cast<uint8_t>(row));
    const auto value = make_bytes(VALUE_WIDTH, static_cast<uint8_t>(row + 128));

    heads.push(store, 0, partition, key.data(), KEY_WIDTH);
    heads.push(store, 1, partition, value.data(), VALUE_WIDTH);

    expected_keys[partition].insert(expected_keys[partition].end(), key.begin(), key.end());
    expected_values[partition].insert(expected_values[partition].end(), value.begin(), value.end());
  }

  heads.finish(store);

  for (auto partition = PartitionId{0}; partition < PARTITION_COUNT; ++partition) {
    EXPECT_EQ(store.key_region(partition).size() / KEY_WIDTH, store.value_region(partition, 0).size() / VALUE_WIDTH);
    expect_region_bytes(store.key_region(partition), expected_keys[partition]);
    expect_region_bytes(store.value_region(partition, 0), expected_values[partition]);
  }
}

TEST_F(ScatterHeadsTest, ScatterWithNullBitmapStream) {
  constexpr auto PARTITION_COUNT = uint32_t{2};
  constexpr auto KEY_WIDTH = size_t{8};
  constexpr auto VALUE_WIDTH = size_t{8};
  constexpr auto BITMAP_WIDTH = size_t{1};

  constexpr auto value_widths = std::array{VALUE_WIDTH};
  auto store = ScatterStore(PartitionCount{PARTITION_COUNT}, KEY_WIDTH, std::span<const size_t>(value_widths),
                            BITMAP_WIDTH, false);
  constexpr auto stream_widths = std::array{KEY_WIDTH, VALUE_WIDTH, BITMAP_WIDTH};
  auto heads = ScatterHeads(PartitionCount{PARTITION_COUNT}, 3, std::span<const size_t>(stream_widths), true);

  constexpr auto fields_per_line = SWWC_LINE_BYTES / KEY_WIDTH;
  constexpr auto rows_per_partition = 2 * fields_per_line + 3;
  constexpr auto row_count = size_t{PARTITION_COUNT} * rows_per_partition;
  auto expected_keys = std::vector<std::vector<std::byte>>(PARTITION_COUNT);
  auto expected_bitmaps = std::vector<std::vector<std::byte>>(PARTITION_COUNT);

  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto partition = static_cast<PartitionId>(row % PARTITION_COUNT);
    const auto key = make_bytes(KEY_WIDTH, static_cast<uint8_t>(row));
    const auto value = make_bytes(VALUE_WIDTH, static_cast<uint8_t>(row + 64));
    const auto bitmap = static_cast<std::byte>(row % 2);

    heads.push(store, 0, partition, key.data(), KEY_WIDTH);
    heads.push(store, 1, partition, value.data(), VALUE_WIDTH);
    heads.push(store, 2, partition, &bitmap, BITMAP_WIDTH);

    expected_keys[partition].insert(expected_keys[partition].end(), key.begin(), key.end());
    expected_bitmaps[partition].push_back(bitmap);
  }

  heads.finish(store);

  for (auto partition = PartitionId{0}; partition < PARTITION_COUNT; ++partition) {
    expect_region_bytes(store.key_region(partition), expected_keys[partition]);
    expect_region_bytes(store.value_null_bitmap_region(partition), expected_bitmaps[partition]);
  }
}

}  // namespace hyrise
