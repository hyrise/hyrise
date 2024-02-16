#include "base_test.hpp"
#include "optimizer/join_ordering/enumerate_ccp.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT

template <typename T>
bool equals(const std::pair<boost::dynamic_bitset<>, boost::dynamic_bitset<>>& lhs, const std::pair<T, T>& rhs) {
  Assert(lhs.first.size() == lhs.second.size() && lhs.first.size() <= sizeof(unsigned long) * 8,  // NOLINT
         "Bitset has too many bits for comparison.");
  return lhs.first.to_ulong() == static_cast<unsigned long>(rhs.first) &&
         lhs.second.to_ulong() == static_cast<unsigned long>(rhs.second);
}
}  // namespace

namespace hyrise {

class EnumerateCcpTest : public BaseTest {};

/**
 * Test that the correct CCPs are enumerated for _very_ simple graphs and that they are enumerated in the correct order
 */

TEST_F(EnumerateCcpTest, Simple) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}};

  const auto pairs = EnumerateCcp{2, edges}();

  ASSERT_EQ(pairs.size(), 1u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b01, 0b10)));
}

TEST_F(EnumerateCcpTest, Chain) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {1, 2}, {2, 3}};

  const auto pairs = EnumerateCcp{4, edges}();

  ASSERT_EQ(pairs.size(), 10u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b0100, 0b1000)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b0010, 0b0100)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b0010, 0b1100)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b0110, 0b1000)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b0001, 0b0010)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b0001, 0b0110)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b0001, 0b1110)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b0011, 0b0100)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b0011, 0b1100)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b0111, 0b1000)));
}

TEST_F(EnumerateCcpTest, Ring) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {1, 2}, {2, 0}};

  const auto pairs = EnumerateCcp{3, edges}();

  ASSERT_EQ(pairs.size(), 6u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b010, 0b100)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b001, 0b100)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b001, 0b010)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b001, 0b110)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b011, 0b100)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b101, 0b010)));
}

TEST_F(EnumerateCcpTest, Star) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {0, 2}, {0, 3}};

  const auto pairs = EnumerateCcp{4, edges}();

  ASSERT_EQ(pairs.size(), 12u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b0001, 0b1000)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b0001, 0b0100)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b0001, 0b0010)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b0011, 0b1000)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b0011, 0b0100)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b0101, 0b1000)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b0101, 0b0010)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b0111, 0b1000)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b1001, 0b0100)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b1001, 0b0010)));
  EXPECT_TRUE(equals(pairs[10], std::make_pair(0b1011, 0b0100)));
  EXPECT_TRUE(equals(pairs[11], std::make_pair(0b1101, 0b0010)));
}

TEST_F(EnumerateCcpTest, Clique) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 1}, {0, 2}, {0, 3}, {1, 2}, {2, 3}, {1, 3}};

  const auto pairs = EnumerateCcp{4, edges}();
  ASSERT_EQ(pairs.size(), 25u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b0100, 0b1000)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b0010, 0b1000)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b0010, 0b0100)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b0010, 0b1100)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b0110, 0b1000)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b1010, 0b0100)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b0001, 0b1000)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b0001, 0b0100)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b0001, 0b1100)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b0001, 0b0010)));
  EXPECT_TRUE(equals(pairs[10], std::make_pair(0b0001, 0b0110)));
  EXPECT_TRUE(equals(pairs[11], std::make_pair(0b0001, 0b1010)));
  EXPECT_TRUE(equals(pairs[12], std::make_pair(0b0001, 0b1110)));
  EXPECT_TRUE(equals(pairs[13], std::make_pair(0b0011, 0b1000)));
  EXPECT_TRUE(equals(pairs[14], std::make_pair(0b0011, 0b0100)));
  EXPECT_TRUE(equals(pairs[15], std::make_pair(0b0011, 0b1100)));
  EXPECT_TRUE(equals(pairs[16], std::make_pair(0b0101, 0b1000)));
  EXPECT_TRUE(equals(pairs[17], std::make_pair(0b0101, 0b0010)));
  EXPECT_TRUE(equals(pairs[18], std::make_pair(0b0101, 0b1010)));
  EXPECT_TRUE(equals(pairs[19], std::make_pair(0b0111, 0b1000)));
  EXPECT_TRUE(equals(pairs[20], std::make_pair(0b1001, 0b0100)));
  EXPECT_TRUE(equals(pairs[21], std::make_pair(0b1001, 0b0010)));
  EXPECT_TRUE(equals(pairs[22], std::make_pair(0b1001, 0b0110)));
  EXPECT_TRUE(equals(pairs[23], std::make_pair(0b1011, 0b0100)));
  EXPECT_TRUE(equals(pairs[24], std::make_pair(0b1101, 0b0010)));
}

TEST_F(EnumerateCcpTest, RandomJoinGraphShape) {
  /**
   *    0
   *   / \
   *  2 - 1 - 3
   */

  std::vector<std::pair<size_t, size_t>> edges{{0, 2}, {0, 1}, {1, 3}, {2, 1}};

  const auto pairs = EnumerateCcp{5, edges}();

  ASSERT_EQ(pairs.size(), 15u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b00010, 0b01000)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b00010, 0b00100)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b00110, 0b01000)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b01010, 0b00100)));
  EXPECT_TRUE(equals(pairs[4], std::make_pair(0b00001, 0b00100)));
  EXPECT_TRUE(equals(pairs[5], std::make_pair(0b00001, 0b00010)));
  EXPECT_TRUE(equals(pairs[6], std::make_pair(0b00001, 0b00110)));
  EXPECT_TRUE(equals(pairs[7], std::make_pair(0b00001, 0b01010)));
  EXPECT_TRUE(equals(pairs[8], std::make_pair(0b00001, 0b01110)));
  EXPECT_TRUE(equals(pairs[9], std::make_pair(0b00011, 0b01000)));
  EXPECT_TRUE(equals(pairs[10], std::make_pair(0b00011, 0b00100)));
  EXPECT_TRUE(equals(pairs[11], std::make_pair(0b00101, 0b00010)));
  EXPECT_TRUE(equals(pairs[12], std::make_pair(0b00101, 0b01010)));
  EXPECT_TRUE(equals(pairs[13], std::make_pair(0b00111, 0b01000)));
  EXPECT_TRUE(equals(pairs[14], std::make_pair(0b01011, 0b00100)));
}

TEST_F(EnumerateCcpTest, ArbitraryVertexNumbering) {
  std::vector<std::pair<size_t, size_t>> edges{{0, 2}, {2, 1}};

  const auto pairs = EnumerateCcp{3, edges}();
  ASSERT_EQ(pairs.size(), 4u);

  EXPECT_TRUE(equals(pairs[0], std::make_pair(0b010, 0b100)));
  EXPECT_TRUE(equals(pairs[1], std::make_pair(0b001, 0b100)));
  EXPECT_TRUE(equals(pairs[2], std::make_pair(0b001, 0b110)));
  EXPECT_TRUE(equals(pairs[3], std::make_pair(0b101, 0b010)));
}

}  // namespace hyrise
