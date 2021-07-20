#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "types.hpp"

// In this domain input modeling is explicitly used.
// https://github.com/hyrise/hyrise/wiki/Input-Domain-Modeling

namespace opossum {

class PartialHashIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions table_column_definitions;
    table_column_definitions.emplace_back("column_1", DataType::String, true);
    table = std::make_shared<Table>(table_column_definitions, TableType::Data);

    values1 = {"hotel", "delta", "nullptr", "delta", "apple", "charlie", "charlie", "inbox"};
    pmr_vector<bool> null_values_1 = {false, false, true, false, false, false, false, false};
    values2 = {"hello", "delta", "funny", "names", "nullptr", "paper", "clock", "inbox"};
    pmr_vector<bool> null_values_2 = {false, false, false, false, true, false, false, false};
    segment1 = std::make_shared<ValueSegment<pmr_string>>(std::move(values1), std::move(null_values_1));
    segment2 = std::make_shared<ValueSegment<pmr_string>>(std::move(values2), std::move(null_values_2));

    Segments segments1 = {segment1};
    Segments segments2 = {segment2};

    table->append_chunk(segments1);
    table->append_chunk(segments2);

    std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
    chunks_to_index.push_back(std::make_pair(ChunkID{0}, table->get_chunk(ChunkID{0})));
    chunks_to_index.push_back(std::make_pair(ChunkID{1}, table->get_chunk(ChunkID{1})));

    index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

    index_map = &(std::dynamic_pointer_cast<PartialHashIndexImpl<pmr_string>>(index->_impl)->_map);
  }

  pmr_vector<pmr_string> values1;
  pmr_vector<pmr_string> values2;
  std::shared_ptr<ValueSegment<pmr_string>> segment1 = nullptr;
  std::shared_ptr<ValueSegment<pmr_string>> segment2 = nullptr;
  std::shared_ptr<Table> table = nullptr;
  std::shared_ptr<PartialHashIndex> index = nullptr;

  /**
   * Use pointers to inner data structures of PartialHashIndex in order to bypass the
   * private scope. Since the variable is set in setup() references are not possible.
   */
  tsl::robin_map<pmr_string, std::vector<RowID>>* index_map = nullptr;
};

TEST_F(PartialHashIndexTest, Type) { EXPECT_EQ(index->type(), SegmentIndexType::PartialHash); }

TEST_F(PartialHashIndexTest, IndexCoverage) {
  EXPECT_EQ(index->get_indexed_chunk_ids(), (std::set<ChunkID>{ChunkID{0}, ChunkID{1}}));

  EXPECT_TRUE(index->is_index_for(ColumnID{0}));
  EXPECT_FALSE(index->is_index_for(ColumnID{1}));
}

TEST_F(PartialHashIndexTest, EmptyInitialization) {
  auto empty_index = PartialHashIndex(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>(), ColumnID{0});

  EXPECT_EQ(empty_index.cbegin(), empty_index.cend());
  EXPECT_EQ(empty_index.null_cbegin(), empty_index.null_cend());

  EXPECT_EQ(empty_index.equals("any").first, empty_index.cend());
  EXPECT_EQ(empty_index.equals("any").second, empty_index.cend());

  EXPECT_EQ(empty_index.get_indexed_chunk_ids().size(), 0);

  EXPECT_TRUE(empty_index.is_index_for(ColumnID{0}));
  EXPECT_FALSE(empty_index.is_index_for(ColumnID{1}));

  EXPECT_THROW(*empty_index.cbegin(), std::logic_error);
  EXPECT_TRUE(empty_index.cbegin().operator==(empty_index.cbegin()));
  EXPECT_FALSE(empty_index.cbegin().operator!=(empty_index.cbegin()));
}

TEST_F(PartialHashIndexTest, MapInitialization) {
  EXPECT_EQ(index_map->size(), 10);

  EXPECT_EQ(index_map->at("hotel").size(), 1);
  EXPECT_EQ(index_map->at("hotel")[0], (RowID{ChunkID{0}, ChunkOffset{0}}));

  EXPECT_EQ(index_map->at("delta").size(), 3);
  EXPECT_EQ(index_map->at("delta")[0], (RowID{ChunkID{0}, ChunkOffset{1}}));
  EXPECT_EQ(index_map->at("delta")[1], (RowID{ChunkID{0}, ChunkOffset{3}}));
  EXPECT_EQ(index_map->at("delta")[2], (RowID{ChunkID{1}, ChunkOffset{1}}));

  EXPECT_EQ(index_map->at("apple").size(), 1);
  EXPECT_EQ(index_map->at("apple")[0], (RowID{ChunkID{0}, ChunkOffset{4}}));

  EXPECT_EQ(index_map->at("charlie").size(), 2);
  EXPECT_EQ(index_map->at("charlie")[0], (RowID{ChunkID{0}, ChunkOffset{5}}));
  EXPECT_EQ(index_map->at("charlie")[1], (RowID{ChunkID{0}, ChunkOffset{6}}));

  EXPECT_EQ(index_map->at("inbox").size(), 2);
  EXPECT_EQ(index_map->at("inbox")[0], (RowID{ChunkID{0}, ChunkOffset{7}}));
  EXPECT_EQ(index_map->at("inbox")[1], (RowID{ChunkID{1}, ChunkOffset{7}}));

  EXPECT_EQ(index_map->at("hello").size(), 1);
  EXPECT_EQ(index_map->at("hello")[0], (RowID{ChunkID{1}, ChunkOffset{0}}));

  EXPECT_EQ(index_map->at("funny").size(), 1);
  EXPECT_EQ(index_map->at("funny")[0], (RowID{ChunkID{1}, ChunkOffset{2}}));

  EXPECT_EQ(index_map->at("names").size(), 1);
  EXPECT_EQ(index_map->at("names")[0], (RowID{ChunkID{1}, ChunkOffset{3}}));

  EXPECT_EQ(index_map->at("paper").size(), 1);
  EXPECT_EQ(index_map->at("paper")[0], (RowID{ChunkID{1}, ChunkOffset{5}}));

  EXPECT_EQ(index_map->at("clock").size(), 1);
  EXPECT_EQ(index_map->at("clock")[0], (RowID{ChunkID{1}, ChunkOffset{6}}));
}

TEST_F(PartialHashIndexTest, Iterators) {
  auto begin = index->cbegin();
  auto end = index->cend();

  auto begin_copy = begin;
  EXPECT_EQ(begin_copy, begin);
  ++begin_copy;
  EXPECT_NE(begin_copy, begin);

  EXPECT_EQ(std::distance(begin, end), 14);
  EXPECT_EQ(std::distance(index->null_cbegin(), index->null_cend()), 2);
  EXPECT_NE(std::find(begin, end, RowID{ChunkID{0}, ChunkOffset{4}}), end);
}

TEST_F(PartialHashIndexTest, NullValues) {
  auto begin = index->null_cbegin();
  auto end = index->null_cend();

  std::multiset<RowID> expected = {RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{4}}};
  std::multiset<RowID> actual = {};

  int size = 0;
  while (begin != end) {
    actual.insert(*begin);
    ++begin;
    ++size;
  }
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(size, 2);
}

TEST_F(PartialHashIndexTest, Add) {
  pmr_vector<pmr_string> values = {"new1", "new2", "new3", "new4", "nullptr", "new6", "new7", "new8"};
  pmr_vector<bool> null_values = {false, false, false, false, true, false, false, false};
  auto segment = std::make_shared<ValueSegment<pmr_string>>(std::move(values), std::move(null_values));
  table->append_chunk(Segments{segment});

  auto chunks_to_add =
      std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{std::make_pair(ChunkID{2}, table->get_chunk(ChunkID{2}))};
  EXPECT_EQ(index->add(chunks_to_add), 1);

  EXPECT_EQ(index->get_indexed_chunk_ids(), (std::set<ChunkID>{ChunkID{0}, ChunkID{1}, ChunkID{2}}));
  EXPECT_EQ(std::distance(index->cbegin(), index->cend()), 21);
  EXPECT_EQ(std::distance(index->null_cbegin(), index->null_cend()), 3);
  EXPECT_EQ(*index->equals("new1").first, (RowID{ChunkID{2}, ChunkOffset{0}}));

  EXPECT_EQ(index->add(chunks_to_add), 0);
}

TEST_F(PartialHashIndexTest, AddToEmpty) {
  auto empty_index = PartialHashIndex(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>(), ColumnID{0});

  auto chunks_to_add =
      std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{std::make_pair(ChunkID{0}, table->get_chunk(ChunkID{0}))};
  EXPECT_EQ(empty_index.add(chunks_to_add), 1);

  EXPECT_EQ(empty_index.get_indexed_chunk_ids(), (std::set<ChunkID>{ChunkID{0}}));
  EXPECT_EQ(std::distance(empty_index.cbegin(), empty_index.cend()), 7);
  EXPECT_EQ(std::distance(empty_index.null_cbegin(), empty_index.null_cend()), 1);
  EXPECT_EQ(*empty_index.equals("hotel").first, (RowID{ChunkID{0}, ChunkOffset{0}}));
}

TEST_F(PartialHashIndexTest, Remove) {
  EXPECT_EQ(index->remove(std::vector<ChunkID>{ChunkID{0}}), 1);

  EXPECT_EQ(index->get_indexed_chunk_ids(), (std::set<ChunkID>{ChunkID{1}}));
  EXPECT_EQ(std::distance(index->cbegin(), index->cend()), 7);
  EXPECT_EQ(std::distance(index->null_cbegin(), index->null_cend()), 1);
  EXPECT_EQ(index->equals("hotel").first, index->cend());

  EXPECT_EQ(index->remove(std::vector<ChunkID>{ChunkID{0}}), 0);
}

TEST_F(PartialHashIndexTest, RemoveFromEmpty) {
  auto empty_index = PartialHashIndex(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>(), ColumnID{0});

  EXPECT_EQ(empty_index.remove(std::vector<ChunkID>{ChunkID{0}}), 0);
  EXPECT_EQ(empty_index.get_indexed_chunk_ids().size(), 0);
  EXPECT_EQ(empty_index.cbegin(), empty_index.cend());
}

TEST_F(PartialHashIndexTest, Values) {
  auto begin = index->cbegin();
  auto end = index->cend();

  std::multiset<RowID> expected = {
      RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{3}},
      RowID{ChunkID{0}, ChunkOffset{4}}, RowID{ChunkID{0}, ChunkOffset{5}}, RowID{ChunkID{0}, ChunkOffset{6}},
      RowID{ChunkID{0}, ChunkOffset{7}}, RowID{ChunkID{1}, ChunkOffset{0}}, RowID{ChunkID{1}, ChunkOffset{1}},
      RowID{ChunkID{1}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{3}}, RowID{ChunkID{1}, ChunkOffset{5}},
      RowID{ChunkID{1}, ChunkOffset{6}}, RowID{ChunkID{1}, ChunkOffset{7}}};
  std::multiset<RowID> actual = {};

  int size = 0;
  while (begin != end) {
    actual.insert(*begin);
    ++begin;
    ++size;
  }

  EXPECT_EQ(actual, expected);
  EXPECT_EQ(size, 14);
}

TEST_F(PartialHashIndexTest, EqualsValue) {
  auto value = "delta";
  auto [begin, end] = index->equals(value);

  std::multiset<RowID> expected = {RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{3}},
                                   RowID{ChunkID{1}, ChunkOffset{1}}};
  std::multiset<RowID> actual = {};

  int size = 0;
  while (begin != end) {
    actual.insert(*begin);
    ++begin;
    ++size;
  }

  EXPECT_EQ(actual, expected);
  EXPECT_EQ(size, 3);
}

TEST_F(PartialHashIndexTest, EqualsValueNotFound) {
  auto value = "invalid";
  auto [begin, end] = index->equals(value);

  EXPECT_EQ(end, begin);
  EXPECT_EQ(end, index->cend());
}

TEST_F(PartialHashIndexTest, NotEqualsValue) {
  auto value = "delta";
  auto pair = index->not_equals(value);
  auto [begin1, end1] = pair.first;
  auto [begin2, end2] = pair.second;

  std::multiset<RowID> expected = {
      RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{4}}, RowID{ChunkID{0}, ChunkOffset{5}},
      RowID{ChunkID{0}, ChunkOffset{6}}, RowID{ChunkID{0}, ChunkOffset{7}}, RowID{ChunkID{1}, ChunkOffset{0}},
      RowID{ChunkID{1}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{3}}, RowID{ChunkID{1}, ChunkOffset{5}},
      RowID{ChunkID{1}, ChunkOffset{6}}, RowID{ChunkID{1}, ChunkOffset{7}}};
  std::multiset<RowID> actual = {};

  int size = 0;
  while (begin1 != end1) {
    actual.insert(*begin1);
    ++begin1;
    ++size;
  }
  while (begin2 != end2) {
    actual.insert(*begin2);
    ++begin2;
    ++size;
  }

  EXPECT_EQ(actual, expected);
  EXPECT_EQ(size, 11);
}

TEST_F(PartialHashIndexTest, NotEqualsValueNotFound) {
  auto value = "invalid";
  auto pair = index->not_equals(value);
  auto [begin1, end1] = pair.first;
  auto [begin2, end2] = pair.second;

  EXPECT_EQ(begin1, index->cbegin());
  EXPECT_EQ(end1, begin2);
  EXPECT_EQ(end2, index->cend());
}

/*
  Test cases:
    MemoryConsumptionNoNulls
    MemoryConsumptionNulls
    MemoryConsumptionMixed
    MemoryConsumptionEmpty
  Tested functions:
    size_t memory_consumption() const;

  |    Characteristic               | Block 1 | Block 2 |
  |---------------------------------|---------|---------|
  |[A] index is empty               |    true |   false |
  |[B] index has NULL positions     |    true |   false |
  |[C] index has non-NULL positions |    true |   false |

  Base Choice:
    A2, B1, C1
  Further derived combinations:
    A2, B1, C2
    A2, B2, C1
   (A1, B1, C1) --infeasible---+
    A1, B2, C2 <-alternative-<-+
*/

// A2, B2, C1
TEST_F(PartialHashIndexTest, MemoryConsumptionNoNulls) {
  auto local_values = pmr_vector<pmr_string>{"h", "d", "f", "d", "a", "c", "c", "i", "b", "z", "x"};
  auto segment = std::make_shared<ValueSegment<pmr_string>>(std::move(local_values));

  Segments segments = {segment};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  //    80 map size (index non-NULL positions)
  // +  80 map size NULL values (index NULL positions)
  // +  72 number of different non-NULL values (9) * hash size (8)
  // + 216 number of different non-NULL values (9) * vector size (24)
  // +  88 number of non-NULL values (11) * sizeof(RowID) (8)
  // +   0 (NULL elements hash size (8) + vector size (24)) * 0
  // +   0 number of NULL elements (0) * sizeof(RowID) (8)
  // +   2 number of indexed columns (1) * sizeof(ColumnID) (2)
  // +  48 chunk ids set
  // +   4 number of indexed chunks (1) * sizeof(ChunkID) (4)
  // +   1 _is_initialized
  // +  16 impl
  // +   1 SegmentIndexType
  // = 608
  EXPECT_EQ(index->memory_consumption(), 608u);
}

// A2, B1, C2
TEST_F(PartialHashIndexTest, MemoryConsumptionNulls) {
  const auto& dict_segment_string_nulls =
      create_dict_segment_by_type<pmr_string>(DataType::String, {std::nullopt, std::nullopt});

  Segments segments = {dict_segment_string_nulls};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  //    80 map size (index non-NULL positions)
  // +  80 map size NULL values (index NULL positions)
  // +   0 number of different non-NULL values (0) * hash size (8)
  // +   0 number of different non-NULL values (0) * vector size (24)
  // +   0 number of non-NULL values (2) * sizeof(RowID) (8)
  // +  32 (NULL elements hash size (8) + vector size (24)) * 1
  // +  16 number of NULL elements (2) * sizeof(RowID) (8)
  // +   2 number of indexed columns (1) * sizeof(ColumnID) (2)
  // +  48 chunk ids set
  // +   4 number of indexed chunks (1) * sizeof(ChunkID) (4)
  // +   1 _is_initialized
  // +  16 impl
  // +   1 SegmentIndexType
  // = 280
  EXPECT_EQ(index->memory_consumption(), 280u);
}

// A2, B1, C1
TEST_F(PartialHashIndexTest, MemoryConsumptionMixed) {
  const auto& dict_segment_string_mixed = create_dict_segment_by_type<pmr_string>(
      DataType::String, {std::nullopt, "h", "d", "f", "d", "a", std::nullopt, std::nullopt, "c", std::nullopt, "c", "i",
                         "b", "z", "x", std::nullopt});

  Segments segments = {dict_segment_string_mixed};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  //    80 map size (index non-NULL positions)
  // +  80 map size NULL values (index NULL positions)
  // +  72 number of different non-NULL values (9) * hash size (8)
  // + 216 number of different non-NULL values (9) * vector size (24)
  // +  88 number of non-NULL values (11) * sizeof(RowID) (8)
  // +  32 (NULL elements hash size (8) + vector size (24)) * 1
  // +  40 number of NULL elements (5) * sizeof(RowID) (8)
  // +   2 number of indexed columns (1) * sizeof(ColumnID) (2)
  // +  48 chunk ids set
  // +   4 number of indexed chunks (1) * sizeof(ChunkID) (4)
  // +   1 _is_initialized
  // +  16 impl
  // +   1 SegmentIndexType
  // = 680
  EXPECT_EQ(index->memory_consumption(), 680u);
}

// A1, B2, C2
TEST_F(PartialHashIndexTest, MemoryConsumptionEmpty) {
  const auto& dict_segment_string_empty = create_dict_segment_by_type<pmr_string>(DataType::String, {});

  Segments segments = {dict_segment_string_empty};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  //    80 map size (index non-NULL positions)
  // +  80 map size NULL values (index NULL positions)
  // +   0 number of different non-NULL values (0) * hash size (8)
  // +   0 number of different non-NULL values (0) * vector size (24)
  // +   0 number of non-NULL values (0) * sizeof(RowID) (8)
  // +   0 (NULL elements hash size (8) + vector size (24)) * 0
  // +   0 number of NULL elements (0) * sizeof(RowID) (8)
  // +   2 number of indexed columns (1) * sizeof(ColumnID) (2)
  // +  48 chunk ids set
  // +   4 number of indexed chunks (1) * sizeof(ChunkID) (4)
  // +   1 _is_initialized
  // +  16 impl
  // +   1 SegmentIndexType
  // = 232
  EXPECT_EQ(index->memory_consumption(), 232u);
}

}  // namespace opossum
