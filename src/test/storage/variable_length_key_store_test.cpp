#include <algorithm>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "storage/index/group_key/variable_length_key_proxy.hpp"
#include "storage/index/group_key/variable_length_key_store.hpp"
#include "types.hpp"

namespace opossum {

class VariableLengthKeyStoreTest : public BaseTest {
 protected:
  void SetUp() override {
    _key_reference = VariableLengthKey(sizeof(uint32_t));
    _key_equal = VariableLengthKey(sizeof(uint32_t));
    _key_less = VariableLengthKey(sizeof(uint32_t));
    _key_greater = VariableLengthKey(sizeof(uint32_t));
    _store = VariableLengthKeyStore(4, sizeof(uint32_t));
    _store_sorted = VariableLengthKeyStore(4, sizeof(uint32_t));

    _key_reference |= 65793u;
    _key_equal |= 65793u;
    _key_less |= 65535u;
    _key_greater |= 131072u;

    _store[0] = _key_reference;
    _store[1] = _key_equal;
    _store[2] = _key_less;
    _store[3] = _key_greater;

    _store_sorted[0] = _key_less;
    _store_sorted[1] = _key_reference;
    _store_sorted[2] = _key_equal;
    _store_sorted[3] = _key_greater;
  }

 protected:
  VariableLengthKey _key_reference;
  VariableLengthKey _key_less;
  VariableLengthKey _key_equal;
  VariableLengthKey _key_greater;
  VariableLengthKeyStore _store;
  VariableLengthKeyStore _store_sorted;
};

TEST_F(VariableLengthKeyStoreTest, CopyIterators) {
  auto mutable_iter = _store.begin();
  auto const_iter = _store.cbegin();

  auto mutable_from_mutable = mutable_iter;
  auto const_from_const = const_iter;
  auto const_from_mutable = VariableLengthKeyStore::const_iterator(mutable_iter);

  EXPECT_TRUE(mutable_from_mutable == mutable_iter);
  EXPECT_TRUE(const_from_const == const_iter);
  EXPECT_TRUE(const_from_mutable == const_iter);
}

TEST_F(VariableLengthKeyStoreTest, CheckEqualityOfIterators) {
  auto begin = _store.begin();
  auto end = _store.end();
  auto cbegin = _store.cbegin();

  EXPECT_TRUE(begin == _store.begin());
  EXPECT_TRUE(begin == _store.cbegin());
  EXPECT_TRUE(cbegin == _store.begin());
  EXPECT_FALSE(begin == end);
  EXPECT_FALSE(cbegin == end);

  EXPECT_FALSE(begin != _store.begin());
  EXPECT_FALSE(begin != _store.cbegin());
  EXPECT_FALSE(cbegin != _store.begin());
  EXPECT_TRUE(begin != end);
  EXPECT_TRUE(cbegin != end);
}

TEST_F(VariableLengthKeyStoreTest, CalculateIteratorDistance) {
  auto begin = _store.begin();
  auto cbegin = _store.cbegin();
  auto end = _store.end();
  auto second = begin + 1;

  EXPECT_EQ(begin - begin, 0);
  EXPECT_EQ(cbegin - begin, 0);
  EXPECT_EQ(second - begin, 1);
  EXPECT_EQ(end - begin, 4);

  EXPECT_EQ(std::distance(begin, begin), 0);
  EXPECT_EQ(std::distance(begin, second), 1);
  EXPECT_EQ(std::distance(begin, end), 4);
}

TEST_F(VariableLengthKeyStoreTest, AdvanceIterator) {
  auto begin = _store.begin();
  auto end = _store.end();
  auto advanced = begin;

  std::advance(advanced, 4);

  EXPECT_EQ(end, advanced);
  EXPECT_EQ(end, begin + 4);
}

TEST_F(VariableLengthKeyStoreTest, DereferenceIterator) {
  auto first = _store.begin();
  auto second = first + 1;
  auto third = second + 1;
  auto fourth = third + 1;
  auto cfirst = _store.cbegin();
  auto csecond = cfirst + 1;
  auto cthird = csecond + 1;
  auto cfourth = cthird + 1;

  EXPECT_TRUE(_key_reference == *first);
  EXPECT_TRUE(_key_equal == *second);
  EXPECT_TRUE(_key_less == *third);
  EXPECT_TRUE(_key_greater == *fourth);

  EXPECT_TRUE(_key_reference == *cfirst);
  EXPECT_TRUE(_key_equal == *csecond);
  EXPECT_TRUE(_key_less == *cthird);
  EXPECT_TRUE(_key_greater == *cfourth);
}

TEST_F(VariableLengthKeyStoreTest, CopyWithStd) {
  auto result = std::vector<VariableLengthKey>(_store.size());
  auto expected = std::vector<VariableLengthKey>{_key_reference, _key_equal, _key_less, _key_greater};
  std::copy(_store.cbegin(), _store.cend(), result.begin());
  EXPECT_EQ(expected, result);
}

TEST_F(VariableLengthKeyStoreTest, SearchWithStd) {
  auto lower_bound_of_less = std::lower_bound(_store_sorted.begin(), _store_sorted.end(), _key_less);
  auto upper_bound_of_less = std::upper_bound(_store_sorted.begin(), _store_sorted.end(), _key_less);
  auto lower_bound_of_equal = std::lower_bound(_store_sorted.begin(), _store_sorted.end(), _key_equal);
  auto upper_bound_of_equal = std::upper_bound(_store_sorted.begin(), _store_sorted.end(), _key_equal);
  auto upper_bound_of_greater = std::upper_bound(_store_sorted.begin(), _store_sorted.end(), _key_greater);

  EXPECT_EQ(_store_sorted.begin() + 0, lower_bound_of_less);
  EXPECT_EQ(_store_sorted.begin() + 1, upper_bound_of_less);
  EXPECT_EQ(_store_sorted.begin() + 1, lower_bound_of_equal);
  EXPECT_EQ(_store_sorted.begin() + 3, upper_bound_of_equal);
  EXPECT_EQ(_store_sorted.end(), upper_bound_of_greater);
}

TEST_F(VariableLengthKeyStoreTest, ReadAccessViaBracketsOperator) {
  EXPECT_TRUE(_store[0] == _key_reference);
  EXPECT_TRUE(_store[1] == _key_equal);
  EXPECT_TRUE(_store[2] == _key_less);
  EXPECT_TRUE(_store[3] == _key_greater);

  const auto& cstore = _store;
  EXPECT_TRUE(cstore[0] == _key_reference);
  EXPECT_TRUE(cstore[1] == _key_equal);
  EXPECT_TRUE(cstore[2] == _key_less);
  EXPECT_TRUE(cstore[3] == _key_greater);
}

TEST_F(VariableLengthKeyStoreTest, WriteAccessViaBracketsOperator) {
  auto value_0 = VariableLengthKey(sizeof(uint32_t));
  auto value_1 = VariableLengthKey(sizeof(uint32_t));
  auto value_2 = VariableLengthKey(sizeof(uint32_t));
  auto value_3 = VariableLengthKey(sizeof(uint32_t));

  value_0 |= 0u;
  value_1 |= 1u;
  value_2 |= 2u;
  value_3 |= 3u;

  _store[0] = value_0;
  _store[1] = value_1;
  _store[2] = value_2;
  _store[3] = value_0;
  _store[3] |= 3u;

  EXPECT_TRUE(_store[0] == value_0);
  EXPECT_TRUE(_store[1] == value_1);
  EXPECT_TRUE(_store[2] == value_2);
  EXPECT_TRUE(_store[3] == value_3);
}

TEST_F(VariableLengthKeyStoreTest, WriteNonFittingKeys) {
  if (!HYRISE_DEBUG) GTEST_SKIP();
  // _store is created with 4 bytes per entry
  auto short_key = VariableLengthKey(sizeof(uint16_t));
  auto long_key = VariableLengthKey(sizeof(uint64_t));

  EXPECT_THROW(_store[0] = short_key, std::logic_error);
  EXPECT_THROW(_store[0] = long_key, std::logic_error);
  EXPECT_THROW(*_store.begin() = short_key, std::logic_error);
  EXPECT_THROW(*_store.begin() = long_key, std::logic_error);
}

}  // namespace opossum
