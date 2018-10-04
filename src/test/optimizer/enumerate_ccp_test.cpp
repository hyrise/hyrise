#include "gtest/gtest.h"

#include "optimizer/join_ordering/enumerate_ccp.hpp"

#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT

bool equals(const std::pair<boost::dynamic_bitset<>, boost::dynamic_bitset<>>& lhs,
            const std::pair<unsigned long, unsigned long>& rhs) {  // NOLINT - doesn't like unsigned long
  Assert(lhs.first.size() == lhs.second.size() && lhs.first.size() <= sizeof(unsigned long) * 8,  // NOLINT
         "Bitset has too many bits for comparison");
  return lhs.first.to_ulong() == rhs.first && lhs.second.to_ulong() == rhs.second;
}
}  // namespace

namespace opossum {

/**
 * Test that the correct CCPs are enumerated for _very_ simple graphs and that they are enumerated in the correct order
 */

TEST(EnumerateCcpTest, Simple) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}};

  const auto pairs = EnumerateCcp{2, edges}();  // NOLINT - {}()

  ASSERT_EQ(pairs.size(), 1u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b01ul, 0b10ul)));
}

TEST(EnumerateCcpTest, Chain) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {1, 2}, {2, 3}};

  const auto pairs = EnumerateCcp{4, edges}();  // NOLINT - {}()

  ASSERT_EQ(pairs.size(), 10u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b0100ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b0010ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b0010ul, 0b1100ul)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b0110ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b0001ul, 0b0010ul)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b0001ul, 0b0110ul)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b0001ul, 0b1110ul)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b0011ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b0011ul, 0b1100ul)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b0111ul, 0b1000ul)));
}

TEST(EnumerateCcpTest, Ring) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {1, 2}, {2, 0}};

  const auto pairs = EnumerateCcp{3, edges}();  // NOLINT - {}()

  ASSERT_EQ(pairs.size(), 6u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b010ul, 0b100ul)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b001ul, 0b100ul)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b001ul, 0b010ul)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b001ul, 0b110ul)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b011ul, 0b100ul)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b101ul, 0b010ul)));
}

TEST(EnumerateCcpTest, Star) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {0, 2}, {0, 3}};

  const auto pairs = EnumerateCcp{4, edges}();  // NOLINT - {}()

  ASSERT_EQ(pairs.size(), 12u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b0001ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b0001ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b0001ul, 0b0010ul)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b0011ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b0011ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b0101ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b0101ul, 0b0010ul)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b0111ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b1001ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b1001ul, 0b0010ul)));
  EXPECT_TRUE(equals(pairs[10], std::make_pair(0b1011ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[11], std::make_pair(0b1101ul, 0b0010ul)));
}

TEST(EnumerateCcpTest, Clique) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {0, 2}, {0, 3}, {1, 2}, {2, 3}, {1, 3}};

  const auto pairs = EnumerateCcp{4, edges}();  // NOLINT - {}()
  ASSERT_EQ(pairs.size(), 25u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b0100ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b0010ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b0010ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b0010ul, 0b1100ul)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b0110ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b1010ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b0001ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b0001ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b0001ul, 0b1100ul)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b0001ul, 0b0010ul)));
  EXPECT_TRUE(equals(pairs[10], std::make_pair(0b0001ul, 0b0110ul)));
  EXPECT_TRUE(equals(pairs[11], std::make_pair(0b0001ul, 0b1010ul)));
  EXPECT_TRUE(equals(pairs[12], std::make_pair(0b0001ul, 0b1110ul)));
  EXPECT_TRUE(equals(pairs[13], std::make_pair(0b0011ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[14], std::make_pair(0b0011ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[15], std::make_pair(0b0011ul, 0b1100ul)));
  EXPECT_TRUE(equals(pairs[16], std::make_pair(0b0101ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[17], std::make_pair(0b0101ul, 0b0010ul)));
  EXPECT_TRUE(equals(pairs[18], std::make_pair(0b0101ul, 0b1010ul)));
  EXPECT_TRUE(equals(pairs[19], std::make_pair(0b0111ul, 0b1000ul)));
  EXPECT_TRUE(equals(pairs[20], std::make_pair(0b1001ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[21], std::make_pair(0b1001ul, 0b0010ul)));
  EXPECT_TRUE(equals(pairs[22], std::make_pair(0b1001ul, 0b0110ul)));
  EXPECT_TRUE(equals(pairs[23], std::make_pair(0b1011ul, 0b0100ul)));
  EXPECT_TRUE(equals(pairs[24], std::make_pair(0b1101ul, 0b0010ul)));
}

TEST(EnumerateCcpTest, RandomJoinGraphShape) {
  /**
   *    0
   *   / \
   *  2 - 1 - 3
   */

  std::vector<std::pair<size_t, size_t>> edges{{0, 2}, {0, 1}, {1, 3}, {2, 1}};

  const auto pairs = EnumerateCcp{5, edges}();  // NOLINT - {}()

  ASSERT_EQ(pairs.size(), 15u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b00010ul, 0b01000ul)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b00010ul, 0b00100ul)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b00110ul, 0b01000ul)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b01010ul, 0b00100ul)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b00001ul, 0b00100ul)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b00001ul, 0b00010ul)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b00001ul, 0b00110ul)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b00001ul, 0b01010ul)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b00001ul, 0b01110ul)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b00011ul, 0b01000ul)));
  EXPECT_TRUE(equals(pairs[10], std::make_pair(0b00011ul, 0b00100ul)));
  EXPECT_TRUE(equals(pairs[11], std::make_pair(0b00101ul, 0b00010ul)));
  EXPECT_TRUE(equals(pairs[12], std::make_pair(0b00101ul, 0b01010ul)));
  EXPECT_TRUE(equals(pairs[13], std::make_pair(0b00111ul, 0b01000ul)));
  EXPECT_TRUE(equals(pairs[14], std::make_pair(0b01011ul, 0b00100ul)));
}

TEST(EnumerateCcpTest, ArbitraryVertexNumbering) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 2}, {2, 1}};

  const auto pairs = EnumerateCcp{3, edges}();  // NOLINT - {}()
  ASSERT_EQ(pairs.size(), 4u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b010ul, 0b100ul)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b001ul, 0b100ul)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b001ul, 0b110ul)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b101ul, 0b010ul)));
}

}  // namespace opossum
