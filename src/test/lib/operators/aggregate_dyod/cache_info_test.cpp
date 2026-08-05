#include <cstdint>

#include "base_test.hpp"
#include "operators/aggregate_dyod/cache_info.hpp"

namespace hyrise {

class CacheInfoTest : public BaseTest {};

TEST_F(CacheInfoTest, SanitizeAppliesFallbacksForUnknownSizes) {
  const auto from_zero = sanitize_cache_sizes(0, 0, 0, 0);
  EXPECT_EQ(from_zero.l1d_bytes, FALLBACK_CACHE_SIZES.l1d_bytes);
  EXPECT_EQ(from_zero.l2_bytes, FALLBACK_CACHE_SIZES.l2_bytes);
  EXPECT_EQ(from_zero.l3_bytes, FALLBACK_CACHE_SIZES.l3_bytes);

  const auto from_error = sanitize_cache_sizes(-1, -1, -1, 0);
  EXPECT_EQ(from_error.l1d_bytes, FALLBACK_CACHE_SIZES.l1d_bytes);
  EXPECT_EQ(from_error.l2_bytes, FALLBACK_CACHE_SIZES.l2_bytes);
  EXPECT_EQ(from_error.l3_bytes, FALLBACK_CACHE_SIZES.l3_bytes);
}

TEST_F(CacheInfoTest, SanitizePassesThroughPlausibleSizes) {
  const auto sizes = sanitize_cache_sizes(48 * 1024, 1'280 * 1024, 32 * 1024 * 1024, 4);
  EXPECT_EQ(sizes.l1d_bytes, 48 * 1024);
  EXPECT_EQ(sizes.l2_bytes, 1'280 * 1024);
  EXPECT_EQ(sizes.l3_bytes, 32 * 1024 * 1024);
}

TEST_F(CacheInfoTest, SanitizeClampsImplausibleSizes) {
  const auto tiny = sanitize_cache_sizes(16, 1024, 4096, 4);
  EXPECT_GE(tiny.l1d_bytes, 4 * 1024);
  EXPECT_GE(tiny.l2_bytes, 64 * 1024);
  EXPECT_GE(tiny.l3_bytes, 1024 * 1024);

  const auto huge = sanitize_cache_sizes(int64_t{1} << 40, int64_t{1} << 40, int64_t{1} << 40, 4);
  EXPECT_LE(huge.l1d_bytes, 1024 * 1024);
  EXPECT_LE(huge.l2_bytes, 64 * 1024 * 1024);
  EXPECT_LE(huge.l3_bytes, 1024 * 1024 * 1024);
}

TEST_F(CacheInfoTest, SanitizeKeepsLevelsOrdered) {
  const auto sizes = sanitize_cache_sizes(512 * 1024, 128 * 1024, 0, 4);
  EXPECT_LE(sizes.l1d_bytes, sizes.l2_bytes);
  EXPECT_LE(sizes.l2_bytes, sizes.l3_bytes);
}

TEST_F(CacheInfoTest, ParseCpuListCountsSinglesAndRanges) {
  EXPECT_EQ(parse_cpu_list_count("0"), 1u);
  EXPECT_EQ(parse_cpu_list_count("0-3"), 4u);
  EXPECT_EQ(parse_cpu_list_count("0,4,8"), 3u);
  EXPECT_EQ(parse_cpu_list_count("0-3,64-67"), 8u);
  EXPECT_EQ(parse_cpu_list_count("0-191"), 192u);
}

TEST_F(CacheInfoTest, ParseCpuListRejectsMalformedLists) {
  EXPECT_EQ(parse_cpu_list_count(""), 0u);
  EXPECT_EQ(parse_cpu_list_count("cpus"), 0u);
  EXPECT_EQ(parse_cpu_list_count("3-0"), 0u);
  EXPECT_EQ(parse_cpu_list_count("0-"), 0u);
  EXPECT_EQ(parse_cpu_list_count("0,"), 0u);
  EXPECT_EQ(parse_cpu_list_count("0-3x"), 0u);
}

TEST_F(CacheInfoTest, SanitizeHandlesTheSharingCount) {
  const auto unknown = sanitize_cache_sizes(0, 0, 0, 0);
  EXPECT_EQ(unknown.llc_sharing_cpus, FALLBACK_CACHE_SIZES.llc_sharing_cpus);

  const auto reported = sanitize_cache_sizes(0, 0, 0, 8);
  EXPECT_EQ(reported.llc_sharing_cpus, 8u);

  const auto absurd = sanitize_cache_sizes(0, 0, 0, 100'000);
  EXPECT_LE(absurd.llc_sharing_cpus, 1024u);
}

TEST_F(CacheInfoTest, CacheSizesAreSanitized) {
  const auto& sizes = cache_sizes();
  EXPECT_GE(sizes.l1d_bytes, 4 * 1024);
  EXPECT_LE(sizes.l1d_bytes, sizes.l2_bytes);
  EXPECT_LE(sizes.l2_bytes, sizes.l3_bytes);
  EXPECT_LE(sizes.l3_bytes, size_t{1024} * 1024 * 1024);
  EXPECT_GE(sizes.llc_sharing_cpus, 1u);
  EXPECT_LE(sizes.llc_sharing_cpus, 1024u);
}

}  // namespace hyrise
