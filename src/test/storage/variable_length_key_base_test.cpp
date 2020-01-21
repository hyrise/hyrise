#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "storage/index/group_key/variable_length_key_base.hpp"
#include "types.hpp"

namespace opossum {

class VariableLengthKeyBaseTest : public BaseTest {
 protected:
  void SetUp() override {
    _reference = 42u;
    _equal = 42u;
    _less = 21u;
    _greater = 84u;
    _shorter = 42u;
    _longer = 42u;
    _reference_key = _create_fitted_key(_reference);
    _equal_key = _create_fitted_key(_equal);
    _less_key = _create_fitted_key(_less);
    _greater_key = _create_fitted_key(_greater);
    _shorter_key = _create_fitted_key(_shorter);
    _longer_key = _create_fitted_key(_longer);
  }

 protected:
  template <typename T>
  static VariableLengthKeyBase _create_fitted_key(T& value) {
    return VariableLengthKeyBase(reinterpret_cast<VariableLengthKeyWord*>(&value), sizeof(value));
  }

 protected:
  uint32_t _reference;
  uint32_t _equal;
  uint32_t _less;
  uint32_t _greater;
  uint16_t _shorter;
  uint64_t _longer;
  VariableLengthKeyBase _reference_key;
  VariableLengthKeyBase _equal_key;
  VariableLengthKeyBase _less_key;
  VariableLengthKeyBase _greater_key;
  VariableLengthKeyBase _shorter_key;
  VariableLengthKeyBase _longer_key;
};

TEST_F(VariableLengthKeyBaseTest, ComparisionEqual) {
  EXPECT_TRUE(_reference_key == _reference_key);
  EXPECT_TRUE(_reference_key == _equal_key);

  EXPECT_FALSE(_reference_key == _less_key);
  EXPECT_FALSE(_reference_key == _greater_key);
  EXPECT_FALSE(_reference_key == _shorter_key);
  EXPECT_FALSE(_reference_key == _longer_key);
}

TEST_F(VariableLengthKeyBaseTest, ComparisionNotEqual) {
  EXPECT_TRUE(_reference_key != _less_key);
  EXPECT_TRUE(_reference_key != _greater_key);
  EXPECT_TRUE(_reference_key != _shorter_key);
  EXPECT_TRUE(_reference_key != _longer_key);

  EXPECT_FALSE(_reference_key != _reference_key);
  EXPECT_FALSE(_reference_key != _equal_key);
}

TEST_F(VariableLengthKeyBaseTest, ComparisionLess) {
  EXPECT_TRUE(_reference_key < _greater_key);
  EXPECT_TRUE(_reference_key < _longer_key);

  EXPECT_FALSE(_reference_key < _reference_key);
  EXPECT_FALSE(_reference_key < _equal_key);
  EXPECT_FALSE(_reference_key < _less_key);
  EXPECT_FALSE(_reference_key < _shorter_key);
}

TEST_F(VariableLengthKeyBaseTest, ComparisionLessEqual) {
  EXPECT_TRUE(_reference_key <= _reference_key);
  EXPECT_TRUE(_reference_key <= _equal_key);
  EXPECT_TRUE(_reference_key <= _greater_key);
  EXPECT_TRUE(_reference_key <= _longer_key);

  EXPECT_FALSE(_reference_key <= _less_key);
  EXPECT_FALSE(_reference_key <= _shorter_key);
}

TEST_F(VariableLengthKeyBaseTest, ComparisionGreater) {
  EXPECT_TRUE(_reference_key > _less_key);
  EXPECT_TRUE(_reference_key > _shorter_key);

  EXPECT_FALSE(_reference_key > _reference_key);
  EXPECT_FALSE(_reference_key > _equal_key);
  EXPECT_FALSE(_reference_key > _greater_key);
  EXPECT_FALSE(_reference_key > _longer_key);
}

TEST_F(VariableLengthKeyBaseTest, ComparisionGreaterEqual) {
  EXPECT_TRUE(_reference_key >= _reference_key);
  EXPECT_TRUE(_reference_key >= _equal_key);
  EXPECT_TRUE(_reference_key >= _less_key);
  EXPECT_TRUE(_reference_key >= _shorter_key);

  EXPECT_FALSE(_reference_key >= _greater_key);
  EXPECT_FALSE(_reference_key >= _longer_key);
}

TEST_F(VariableLengthKeyBaseTest, OrAssignment) {
  uint32_t expected = 0x00000000u;
  uint32_t actual = 0x00000000u;
  auto actual_key = _create_fitted_key(actual);

  expected |= 0x00000001u;
  actual_key |= 0x00000001u;
  EXPECT_EQ(expected, actual);

  expected |= 0x0000000Fu;
  actual_key |= 0x0000000Fu;
  EXPECT_EQ(expected, actual);

  expected |= 0x000000F0u;
  actual_key |= 0x000000F0u;
  EXPECT_EQ(expected, actual);

  expected |= 0xFFFFFFFFu;
  actual_key |= 0xFFFFFFFFu;
  EXPECT_EQ(expected, actual);
}

TEST_F(VariableLengthKeyBaseTest, OrAssignmentWithKeyLongerThan64Bit) {
  uint64_t memory[2] = {0x0000000000000000u, 0x0000000000000000u};
  auto key = VariableLengthKeyBase(reinterpret_cast<VariableLengthKeyWord*>(&memory), sizeof(memory));

  key |= 0xFF00FF00F0F0FF00u;

  uint64_t expected_low = 0xFF00FF00F0F0FF00u;
  uint64_t expected_high = 0x0000000000000000u;

  if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    EXPECT_EQ(expected_high, memory[1]);
    EXPECT_EQ(expected_low, memory[0]);
  } else {
    EXPECT_EQ(expected_high, memory[0]);
    EXPECT_EQ(expected_low, memory[1]);
  }
}

TEST_F(VariableLengthKeyBaseTest, ShiftAssignment) {
  uint32_t expected = 0xF0F0FF00u;
  uint32_t actual = 0xF0F0FF00u;
  auto actual_key = _create_fitted_key(actual);

  expected <<= 0;
  actual_key <<= 0;
  EXPECT_EQ(expected, actual);

  expected <<= 8;
  actual_key <<= 8;
  EXPECT_EQ(expected, actual);

  expected <<= 16;
  actual_key <<= 16;
  EXPECT_EQ(expected, actual);

  expected <<= 16;
  actual_key <<= 16;
  EXPECT_EQ(expected, actual);
}

TEST_F(VariableLengthKeyBaseTest, ShiftAndSet) {
  uint64_t memory = 0xFF000000F0F0FF00u;
  VariableLengthKeyBase key;
  // create key pointing to lower half of memory
  if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    key = VariableLengthKeyBase(reinterpret_cast<VariableLengthKeyWord*>(&memory), sizeof(uint32_t));
  } else {
    key = VariableLengthKeyBase(reinterpret_cast<VariableLengthKeyWord*>(&memory) + sizeof(uint32_t), sizeof(uint32_t));
  }

  uint8_t small_value = 0xFFu;
  key.shift_and_set(small_value, sizeof(uint8_t) * 8);
  EXPECT_EQ(0xFF000000F0FF00FFu, memory);

  uint32_t equal_value = 0xF0F0F0F0u;
  key.shift_and_set(equal_value, sizeof(uint32_t) * 8);
  EXPECT_EQ(0xFF000000F0F0F0F0u, memory);

  uint64_t large_value = 0xFF00FF00FF00FF00u;
  key.shift_and_set(large_value, sizeof(uint64_t) * 8);
  EXPECT_EQ(0xFF000000FF00FF00, memory);
}

}  // namespace opossum
