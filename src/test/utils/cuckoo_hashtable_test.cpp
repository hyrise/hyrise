#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "utils/cuckoo_hashtable.hpp"

namespace opossum {

class CuckooHashtableTest : public BaseTest {};

TEST_F(CuckooHashtableTest, BasicPutAndGet) {
  auto hashtable = std::make_shared<HashTable<int32_t>>(2);
  hashtable->put(5, RowID{ChunkID{0}, 0});
  hashtable->put(6, RowID{ChunkID{0}, 0});

  EXPECT_TRUE(hashtable->get(5));
  EXPECT_TRUE(hashtable->get(6));
  EXPECT_FALSE(hashtable->get(7));
}

TEST_F(CuckooHashtableTest, StackRowIDs) {
  auto hashtable = std::make_shared<HashTable<int32_t>>(2);

  hashtable->put(5, RowID{ChunkID{0}, 12});
  auto row_ids = hashtable->get(5);
  EXPECT_TRUE(row_ids);
  EXPECT_EQ(row_ids->get().type(), typeid(RowID));
  EXPECT_EQ(boost::get<RowID>(row_ids->get()), (RowID{ChunkID{0}, 12}));

  hashtable->put(5, RowID{ChunkID{0}, 13});
  row_ids = hashtable->get(5);
  EXPECT_TRUE(row_ids);
  EXPECT_EQ(row_ids->get().type(), typeid(PosList));
  EXPECT_EQ(boost::get<PosList>(row_ids->get()).size(), 2u);
  EXPECT_EQ(boost::get<PosList>(row_ids->get())[0], (RowID{ChunkID{0}, 12}));
  EXPECT_EQ(boost::get<PosList>(row_ids->get())[1], (RowID{ChunkID{0}, 13}));
}

/*
This test is rather useless as it is, because it does not produce a collision that is valid for all used hash functions.
For example, 4 and 3617331 collide for the first function, but not for the second. We have not faced a situation yet,
in which all internal hash function collide for the same values.
However, it would definitely make sense to add a test with appropriate values, as soon as we find them.
*/
TEST_F(CuckooHashtableTest, HandleCollision) {
  auto table_size = 4;
  auto hashtable = std::make_shared<HashTable<int32_t>>(table_size);
  /*
  All these 4 integers should produce the same position for the first hash function, thus there is a collision.
  This test ensures that the values are properly inserted by using the other hash functions.
  */
  hashtable->put(4, RowID{ChunkID{0}, 0});
  hashtable->put(3617331, RowID{ChunkID{0}, 0});
  hashtable->put(5346671, RowID{ChunkID{0}, 0});
  hashtable->put(6165505, RowID{ChunkID{0}, 0});

  EXPECT_TRUE(hashtable->get(4));
  EXPECT_TRUE(hashtable->get(3617331));
  EXPECT_TRUE(hashtable->get(5346671));
  EXPECT_TRUE(hashtable->get(6165505));
}

}  // namespace opossum
