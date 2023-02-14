#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

#include "base_test.hpp"

#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "types.hpp"

namespace hyrise {

class PartialHashIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto table_column_definitions = TableColumnDefinitions{{"column_1", DataType::String, true}};
    table = std::make_shared<Table>(table_column_definitions, TableType::Data);

    values1 = {"hotel", "delta", "nullptr", "delta", "apple", "charlie", "charlie", "inbox"};
    auto null_values_1 = {false, false, true, false, false, false, false, false};
    values2 = {"hello", "delta", "funny", "names", "nullptr", "paper", "clock", "inbox"};
    auto null_values_2 = {false, false, false, false, true, false, false, false};
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
    index_map = &(static_cast<PartialHashIndexImpl<pmr_string>*>(index->_impl.get())->_positions);
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
  tsl::sparse_map<pmr_string, std::vector<RowID>>* index_map = nullptr;

  // Utility functions to access an index's protected functions.
  PartialHashIndex::Iterator cbegin(std::shared_ptr<PartialHashIndex> index) const {
    return index->_cbegin();
  }

  PartialHashIndex::Iterator cend(std::shared_ptr<PartialHashIndex> index) const {
    return index->_cend();
  }

  PartialHashIndex::Iterator null_cbegin(std::shared_ptr<PartialHashIndex> index) const {
    return index->_null_cbegin();
  }

  PartialHashIndex::Iterator null_cend(std::shared_ptr<PartialHashIndex> index) const {
    return index->_null_cend();
  }

  PartialHashIndex::IteratorRange range_equals(std::shared_ptr<PartialHashIndex> index,
                                               const AllTypeVariant& value) const {
    return index->_range_equals(value);
  }

  PartialHashIndex::IteratorRangePair range_not_equals(std::shared_ptr<PartialHashIndex> index,
                                                       const AllTypeVariant& value) const {
    return index->_range_not_equals(value);
  }
};

TEST_F(PartialHashIndexTest, IndexCoverage) {
  EXPECT_EQ(index->get_indexed_chunk_ids().size(), 2);
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{0}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{1}));

  EXPECT_TRUE(index->is_index_for(ColumnID{0}));
  EXPECT_FALSE(index->is_index_for(ColumnID{1}));
}

TEST_F(PartialHashIndexTest, EmptyInitialization) {
  EXPECT_THROW(PartialHashIndex(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>(), ColumnID{0}),
               std::logic_error);
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
  auto begin = cbegin(index);
  auto end = cend(index);

  auto begin_copy = begin;
  EXPECT_EQ(begin_copy, begin);
  ++begin_copy;
  EXPECT_NE(begin_copy, begin);

  // Test size of index iterator.
  EXPECT_EQ(std::distance(begin, end), 14);
  // Test size of NULL values index iterator.
  EXPECT_EQ(std::distance(null_cbegin(index), null_cend(index)), 2);
  // Test for existing value in iterator.
  EXPECT_NE(std::find(begin, end, RowID{ChunkID{0}, ChunkOffset{4}}), end);
  // Test for not-existing value in iterator.
  EXPECT_EQ(std::find(begin, end, RowID{ChunkID{2}, ChunkOffset{4}}), end);
}

TEST_F(PartialHashIndexTest, AccessValuesWithIterators) {
  auto test_access_values_with_iterators = [](auto begin, auto end) {
    auto current_iter = begin;
    EXPECT_EQ(current_iter, begin);
    ++current_iter;
    EXPECT_NE(current_iter, begin);
    // Test size of index iterator.
    EXPECT_EQ(std::distance(begin, end), 14);
    // Test for existing value in iterator.
    EXPECT_NE(std::find(begin, end, RowID{ChunkID{0}, ChunkOffset{4}}), end);
    // Test for not-existing value in iterator.
    EXPECT_EQ(std::find(begin, end, RowID{ChunkID{2}, ChunkOffset{4}}), end);
  };
  index->access_values_with_iterators(test_access_values_with_iterators);
}

TEST_F(PartialHashIndexTest, AccessNullValuesWithIterators) {
  auto access_null_values_with_iterators = [](auto begin, auto end) {
    // Test size of NULL values index iterator.
    EXPECT_EQ(std::distance(begin, end), 2);
  };
  index->access_null_values_with_iterators(access_null_values_with_iterators);
}

TEST_F(PartialHashIndexTest, RangeEqualsWithIteratorExistingValue) {
  auto access_range_equals_with_iterators = [](auto begin, auto end) {
    EXPECT_EQ(std::distance(begin, end), 3);

    EXPECT_EQ(*begin, (RowID{ChunkID{0}, ChunkOffset{1}}));
    ++begin;
    EXPECT_EQ(*begin, (RowID{ChunkID{0}, ChunkOffset{3}}));
    ++begin;
    EXPECT_EQ(*begin, (RowID{ChunkID{1}, ChunkOffset{1}}));
  };
  index->range_equals_with_iterators(access_range_equals_with_iterators, "delta");
}

TEST_F(PartialHashIndexTest, RangeEqualsWithIteratorNotExistingValue) {
  auto access_range_equals_non_existing_value_with_iterators = [&](auto begin, auto end) {
    EXPECT_EQ(begin, cend(index));
    EXPECT_EQ(end, cend(index));
  };
  index->range_equals_with_iterators(access_range_equals_non_existing_value_with_iterators, "blub");
}

TEST_F(PartialHashIndexTest, RangeNotEqualsWithIteratorExistingValue) {
  auto size = size_t{0};
  std::multiset<RowID> expected_when_value_exists = {
      RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{3}},
      RowID{ChunkID{0}, ChunkOffset{4}}, RowID{ChunkID{0}, ChunkOffset{5}}, RowID{ChunkID{0}, ChunkOffset{6}},
      RowID{ChunkID{0}, ChunkOffset{7}}, RowID{ChunkID{1}, ChunkOffset{0}}, RowID{ChunkID{1}, ChunkOffset{1}},
      RowID{ChunkID{1}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{5}}, RowID{ChunkID{1}, ChunkOffset{6}},
      RowID{ChunkID{1}, ChunkOffset{7}}};
  std::multiset<RowID> actual_when_value_exists = {};

  auto access_range_not_equals_with_iterators = [&](auto begin, auto end) {
    for (auto it = begin; it != end; ++it) {
      ++size;
      actual_when_value_exists.insert(*it);
    }
  };

  index->range_not_equals_with_iterators(access_range_not_equals_with_iterators, "names");
  EXPECT_EQ(actual_when_value_exists, expected_when_value_exists);
  EXPECT_EQ(size, 13);
}

TEST_F(PartialHashIndexTest, RangeNotEqualsWithIteratorNotExistingValue) {
  auto size = size_t{0};
  std::set<RowID> expected_when_value_not_exists = {
      RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{3}},
      RowID{ChunkID{0}, ChunkOffset{4}}, RowID{ChunkID{0}, ChunkOffset{5}}, RowID{ChunkID{0}, ChunkOffset{6}},
      RowID{ChunkID{0}, ChunkOffset{7}}, RowID{ChunkID{1}, ChunkOffset{0}}, RowID{ChunkID{1}, ChunkOffset{1}},
      RowID{ChunkID{1}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{3}}, RowID{ChunkID{1}, ChunkOffset{5}},
      RowID{ChunkID{1}, ChunkOffset{6}}, RowID{ChunkID{1}, ChunkOffset{7}}};
  std::set<RowID> actual_when_value_not_exists = {};

  auto access_range_not_equals_non_existing_value_with_iterators = [&](auto begin, auto end) {
    for (auto it = begin; it != end; ++it) {
      ++size;
      actual_when_value_not_exists.insert(*it);
    }
  };
  index->range_not_equals_with_iterators(access_range_not_equals_non_existing_value_with_iterators, "blub");

  EXPECT_EQ(actual_when_value_not_exists, expected_when_value_not_exists);
  EXPECT_EQ(size, 14);
}

TEST_F(PartialHashIndexTest, NullValues) {
  auto begin = null_cbegin(index);
  auto end = null_cend(index);

  std::multiset<RowID> expected = {RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{4}}};
  std::multiset<RowID> actual = {};

  auto size = size_t{0};
  while (begin != end) {
    actual.insert(*begin);
    ++begin;
    ++size;
  }
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(size, 2);
}

TEST_F(PartialHashIndexTest, IndexedNullValues) {
  auto table_column_definitions = TableColumnDefinitions{};
  table_column_definitions.emplace_back("column_1", DataType::String, true);

  auto new_values1 = pmr_vector<pmr_string>{"hotel", "delta", "gamma", "delta", "apple", "charlie", "charlie", "inbox"};
  auto new_null_values_1 = {false, false, false, false, false, false, false, false};
  auto new_values2 = pmr_vector<pmr_string>{"hello", "delta", "funny", "names", "nullptr", "paper", "clock", "inbox"};
  auto new_null_values_2 = {false, false, false, false, true, false, false, false};

  auto new_segment1 = std::make_shared<ValueSegment<pmr_string>>(std::move(new_values1), std::move(new_null_values_1));
  auto new_segment2 = std::make_shared<ValueSegment<pmr_string>>(std::move(new_values2), std::move(new_null_values_2));

  // The first segment contains no null values, the second one does.
  const auto new_segments1 = Segments{new_segment1};
  const auto new_segments2 = Segments{new_segment2};

  auto new_table = std::make_shared<Table>(table_column_definitions, TableType::Data);
  new_table->append_chunk(new_segments1);
  new_table->append_chunk(new_segments2);

  auto chunks_to_index0 = std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{};
  auto chunks_to_index1 = std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{};

  chunks_to_index0.push_back(std::make_pair(ChunkID{0}, new_table->get_chunk(ChunkID{0})));
  chunks_to_index1.push_back(std::make_pair(ChunkID{1}, new_table->get_chunk(ChunkID{1})));

  auto new_index = std::make_shared<PartialHashIndex>(chunks_to_index0, ColumnID{0});
  EXPECT_FALSE(new_index->indexed_null_values());

  new_index->insert_entries(chunks_to_index1);
  EXPECT_TRUE(new_index->indexed_null_values());
}

TEST_F(PartialHashIndexTest, Add) {
  pmr_vector<pmr_string> values = {"new1", "new2", "new3", "new4", "nullptr", "new6", "new7", "new8"};
  pmr_vector<bool> null_values = {false, false, false, false, true, false, false, false};
  auto segment = std::make_shared<ValueSegment<pmr_string>>(std::move(values), std::move(null_values));
  table->append_chunk(Segments{segment});

  auto chunks_to_add =
      std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{std::make_pair(ChunkID{2}, table->get_chunk(ChunkID{2}))};
  EXPECT_EQ(index->insert_entries(chunks_to_add), 1);

  EXPECT_EQ(index->get_indexed_chunk_ids().size(), 3);
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{0}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{1}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{2}));

  EXPECT_EQ(std::distance(cbegin(index), cend(index)), 21);
  EXPECT_EQ(std::distance(null_cbegin(index), null_cend(index)), 3);
  EXPECT_EQ(*range_equals(index, "new1").first, (RowID{ChunkID{2}, ChunkOffset{0}}));

  EXPECT_EQ(index->insert_entries(chunks_to_add), 0);
}

TEST_F(PartialHashIndexTest, Remove) {
  EXPECT_EQ(index->remove_entries(std::vector<ChunkID>{ChunkID{0}}), 1);

  EXPECT_EQ(index->get_indexed_chunk_ids(), (std::unordered_set<ChunkID>{ChunkID{1}}));
  EXPECT_EQ(std::distance(cbegin(index), cend(index)), 7);
  EXPECT_EQ(std::distance(null_cbegin(index), null_cend(index)), 1);
  EXPECT_EQ(range_equals(index, "hotel").first, cend(index));

  EXPECT_EQ(index->remove_entries(std::vector<ChunkID>{ChunkID{0}}), 0);
}

TEST_F(PartialHashIndexTest, ReadAndWriteConcurrentlyStressTest) {
  auto chunks_to_add = std::vector<std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>>{};

  pmr_vector<pmr_string> values2 = {"new1", "new2", "new3", "new4", "nullptr", "new6", "new7", "new8"};
  pmr_vector<bool> null_values2 = {false, false, false, false, true, false, false, false};
  auto segment2 = std::make_shared<ValueSegment<pmr_string>>(std::move(values2), std::move(null_values2));
  table->append_chunk(Segments{segment2});

  chunks_to_add.emplace_back(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{
      std::make_pair(ChunkID{2}, table->get_chunk(ChunkID{2}))});

  pmr_vector<pmr_string> values3 = {"1", "2", "3", "4", "nullptr", "6", "7", "8"};
  pmr_vector<bool> null_values3 = {false, false, false, false, true, false, false, false};
  auto segment3 = std::make_shared<ValueSegment<pmr_string>>(std::move(values3), std::move(null_values3));
  table->append_chunk(Segments{segment3});

  chunks_to_add.emplace_back(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{
      std::make_pair(ChunkID{3}, table->get_chunk(ChunkID{3}))});

  pmr_vector<pmr_string> values4 = {"old1", "old2", "old3", "old4", "oldlptr", "old6", "old7", "old8"};
  pmr_vector<bool> null_values4 = {false, false, false, false, false, false, false, false};
  auto segment4 = std::make_shared<ValueSegment<pmr_string>>(std::move(values4), std::move(null_values4));
  table->append_chunk(Segments{segment4});

  chunks_to_add.emplace_back(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{
      std::make_pair(ChunkID{4}, table->get_chunk(ChunkID{4}))});

  pmr_vector<pmr_string> values5 = {"nullptr", "new2", "new3", "new4", "nullptr", "new6", "new7", "nullptr"};
  pmr_vector<bool> null_values5 = {true, false, false, false, true, false, false, true};
  auto segment5 = std::make_shared<ValueSegment<pmr_string>>(std::move(values5), std::move(null_values5));
  table->append_chunk(Segments{segment5});

  chunks_to_add.emplace_back(std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{
      std::make_pair(ChunkID{5}, table->get_chunk(ChunkID{5}))});

  auto insert_entries_to_index = [&](const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunk_to_add) {
    index->insert_entries(chunk_to_add);
  };

  auto read_from_index = [&]() {
    auto read_from_index_functor = [](auto begin, auto end) {
      for (auto iterator = begin; iterator != end; ++iterator) {
        auto data = *iterator;
        static_cast<void>(data);
      }
    };

    index->access_values_with_iterators(read_from_index_functor);
  };

  constexpr auto N_THREADS = uint8_t{8};
  auto threads = std::vector<std::thread>(N_THREADS);

  for (auto thread_number = uint8_t{0}; thread_number < N_THREADS; ++thread_number) {
    if (thread_number % 2 == 1) {
      threads[thread_number] = std::thread(insert_entries_to_index, chunks_to_add[(thread_number - 1) / 2]);
    } else {
      threads[thread_number] = std::thread(read_from_index);
    }
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(index->get_indexed_chunk_ids().size(), 6);
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{0}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{1}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{2}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{3}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{4}));
  EXPECT_TRUE(index->get_indexed_chunk_ids().contains(ChunkID{5}));

  EXPECT_EQ(std::distance(cbegin(index), cend(index)), 41);
  EXPECT_EQ(std::distance(null_cbegin(index), null_cend(index)), 7);
  EXPECT_EQ(*range_equals(index, "new1").first, (RowID{ChunkID{2}, ChunkOffset{0}}));
}

TEST_F(PartialHashIndexTest, ParallelWritesStressTest) {
  pmr_vector<pmr_string> values2 = {"new1", "new2", "new3", "new4", "nullptr", "new6", "new7", "new8"};
  pmr_vector<bool> null_values2 = {false, false, false, false, true, false, false, false};
  auto segment2 = std::make_shared<ValueSegment<pmr_string>>(std::move(values2), std::move(null_values2));
  table->append_chunk(Segments{segment2});

  auto chunks_to_add =
      std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{std::make_pair(ChunkID{2}, table->get_chunk(ChunkID{2}))};

  auto insert_entries_to_index = [&]() { index->insert_entries(chunks_to_add); };

  constexpr auto N_THREADS = uint8_t{8};
  auto threads = std::vector<std::thread>(N_THREADS);

  for (auto thread_number = uint8_t{0}; thread_number < N_THREADS; ++thread_number) {
    threads[thread_number] = std::thread(insert_entries_to_index);
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(index_map->operator[]("new1").size(), 1);
}

TEST_F(PartialHashIndexTest, Values) {
  auto begin = cbegin(index);
  auto end = cend(index);

  std::multiset<RowID> expected = {
      RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{3}},
      RowID{ChunkID{0}, ChunkOffset{4}}, RowID{ChunkID{0}, ChunkOffset{5}}, RowID{ChunkID{0}, ChunkOffset{6}},
      RowID{ChunkID{0}, ChunkOffset{7}}, RowID{ChunkID{1}, ChunkOffset{0}}, RowID{ChunkID{1}, ChunkOffset{1}},
      RowID{ChunkID{1}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{3}}, RowID{ChunkID{1}, ChunkOffset{5}},
      RowID{ChunkID{1}, ChunkOffset{6}}, RowID{ChunkID{1}, ChunkOffset{7}}};
  std::multiset<RowID> actual = {};

  auto size = uint32_t{0};
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
  auto [begin, end] = range_equals(index, value);

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
  auto [begin, end] = range_equals(index, value);

  EXPECT_EQ(end, begin);
  EXPECT_EQ(end, cend(index));
}

TEST_F(PartialHashIndexTest, NotEqualsValue) {
  auto value = "delta";
  auto pair = range_not_equals(index, value);
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
  auto pair = range_not_equals(index, value);
  auto [begin1, end1] = pair.first;
  auto [begin2, end2] = pair.second;

  EXPECT_EQ(begin1, cbegin(index));
  EXPECT_EQ(end1, begin2);
  EXPECT_EQ(end2, cend(index));
}

/*
  Test cases:
    MemoryUsageNoNulls
    MemoryUsageNulls
    MemoryUsageMixed
    MemoryUsageEmpty
    MemoryUsageNoChunk
  Tested functions:
    size_t estimate_memory_usage() const;

  |    Characteristic                                | Block 1 | Block 2 |
  |--------------------------------------------------|---------|---------|
  |[A] index is empty, i.e., it has no index entries |    true |   false |
  |[B] index has NULL positions                      |    true |   false |
  |[C] index has non-NULL positions                  |    true |   false |
  |[D] index was created for at least one chunk      |    true |   false |

  Base Choice:
    A2, B1, C1, D1
  Further derived combinations:
    A2, B1, C2, D1
    A2, B2, C1, D1
    A1, B2, C2, D1
    A1, B2, C2, D2
    D2 => A1, A1 => B2 && C2, therefore there are no more options.
*/

// A2, B2, C1, D1
TEST_F(PartialHashIndexTest, MemoryUsageNoNulls) {
  auto local_values = pmr_vector<pmr_string>{"h", "d", "f", "d", "a", "c", "c", "i", "b", "z", "x"};
  auto segment = std::make_shared<ValueSegment<pmr_string>>(std::move(local_values));

  Segments segments = {segment};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  auto expected_memory_usage = size_t{0};
  // + indexed ColumnID
  expected_memory_usage += sizeof(ColumnID);
  // + data access mutex
  expected_memory_usage += sizeof(std::shared_mutex);
  // + pointer to BaseParialHashIndexImpl
  expected_memory_usage += sizeof(std::unique_ptr<BasePartialHashIndexImpl>);
  // +  ChunkIDs set
  expected_memory_usage += sizeof(std::unordered_set<ChunkID>);
  // +  number of indexed chunks * ChunkID
  expected_memory_usage += 1 * sizeof(ChunkID);
  // + map size
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * tuple size
  expected_memory_usage += 9 * sizeof(std::pair<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * vector size
  expected_memory_usage += 9 * sizeof(std::vector<RowID>);
  // + number of non-NULL values * RowID
  expected_memory_usage += 11 * sizeof(RowID);
  // + vector size NULL values (index NULL positions)
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + vector size when NULL values are indexed
  expected_memory_usage += 0 * sizeof(std::vector<RowID>);
  // + number of NULL values * RowID
  expected_memory_usage += 0 * sizeof(RowID);

  EXPECT_EQ(index->estimate_memory_usage(), expected_memory_usage);
}

// A2, B1, C2, D1
TEST_F(PartialHashIndexTest, MemoryUsageNulls) {
  const auto& dict_segment_string_nulls =
      create_dict_segment_by_type<pmr_string>(DataType::String, {std::nullopt, std::nullopt});

  Segments segments = {dict_segment_string_nulls};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  auto expected_memory_usage = size_t{0};
  // + indexed ColumnID
  expected_memory_usage += sizeof(ColumnID);
  // + data access mutex
  expected_memory_usage += sizeof(std::shared_mutex);
  // + pointer to BaseParialHashIndexImpl
  expected_memory_usage += sizeof(std::unique_ptr<BasePartialHashIndexImpl>);
  // +  ChunkIDs set
  expected_memory_usage += sizeof(std::unordered_set<ChunkID>);
  // +  number of indexed chunks * ChunkID
  expected_memory_usage += 1 * sizeof(ChunkID);
  // + map size
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * tuple size
  expected_memory_usage += 0 * sizeof(std::pair<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * vector size
  expected_memory_usage += 0 * sizeof(std::vector<RowID>);
  // + number of non-NULL values * RowID
  expected_memory_usage += 0 * sizeof(RowID);
  // + vector size NULL values (index NULL positions)
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + vector size when NULL values are indexed
  expected_memory_usage += 1 * sizeof(std::vector<RowID>);
  // + number of NULL values * RowID
  expected_memory_usage += 2 * sizeof(RowID);

  EXPECT_EQ(index->estimate_memory_usage(), expected_memory_usage);
}

// A2, B1, C1, D1
TEST_F(PartialHashIndexTest, MemoryUsageMixed) {
  const auto& dict_segment_string_mixed = create_dict_segment_by_type<pmr_string>(
      DataType::String, {std::nullopt, "h", "d", "f", "d", "a", std::nullopt, std::nullopt, "c", std::nullopt, "c", "i",
                         "b", "z", "x", std::nullopt});

  Segments segments = {dict_segment_string_mixed};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  auto expected_memory_usage = size_t{0};
  // + indexed ColumnID
  expected_memory_usage += sizeof(ColumnID);
  // + data access mutex
  expected_memory_usage += sizeof(std::shared_mutex);
  // + pointer to BaseParialHashIndexImpl
  expected_memory_usage += sizeof(std::unique_ptr<BasePartialHashIndexImpl>);
  // +  ChunkIDs set
  expected_memory_usage += sizeof(std::unordered_set<ChunkID>);
  // +  number of indexed chunks * ChunkID
  expected_memory_usage += 1 * sizeof(ChunkID);
  // + map size
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * tuple size
  expected_memory_usage += 9 * sizeof(std::pair<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * vector size
  expected_memory_usage += 9 * sizeof(std::vector<RowID>);
  // + number of non-NULL values * RowID
  expected_memory_usage += 11 * sizeof(RowID);
  // + vector size NULL values (index NULL positions)
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + vector size when NULL values are indexed
  expected_memory_usage += 1 * sizeof(std::vector<RowID>);
  // + number of NULL values * RowID
  expected_memory_usage += 5 * sizeof(RowID);

  EXPECT_EQ(index->estimate_memory_usage(), expected_memory_usage);
}

// A1, B2, C2, D1
TEST_F(PartialHashIndexTest, MemoryUsageEmpty) {
  const auto& dict_segment_string_empty = create_dict_segment_by_type<pmr_string>(DataType::String, {});

  Segments segments = {dict_segment_string_empty};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});

  auto expected_memory_usage = size_t{0};
  // + indexed ColumnID
  expected_memory_usage += sizeof(ColumnID);
  // + data access mutex
  expected_memory_usage += sizeof(std::shared_mutex);
  // + pointer to BaseParialHashIndexImpl
  expected_memory_usage += sizeof(std::unique_ptr<BasePartialHashIndexImpl>);
  // +  ChunkIDs set
  expected_memory_usage += sizeof(std::unordered_set<ChunkID>);
  // +  number of indexed chunks * ChunkID
  expected_memory_usage += 1 * sizeof(ChunkID);
  // + map size
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * tuple size
  expected_memory_usage += 0 * sizeof(pmr_string);
  // + number of different non-NULL values * vector size
  expected_memory_usage += 0 * sizeof(std::vector<RowID>);
  // + number of non-NULL values * RowID
  expected_memory_usage += 0 * sizeof(RowID);
  // + vector size NULL values (index NULL positions)
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + vector size when NULL values are indexed
  expected_memory_usage += 0 * sizeof(std::vector<RowID>);
  // + number of NULL values * RowID
  expected_memory_usage += 0 * sizeof(RowID);

  EXPECT_EQ(index->estimate_memory_usage(), expected_memory_usage);
}

TEST_F(PartialHashIndexTest, MemoryUsageNoChunk) {
  const auto& dict_segment_string_empty = create_dict_segment_by_type<pmr_string>(DataType::String, {});

  Segments segments = {dict_segment_string_empty};
  auto chunk = std::make_shared<Chunk>(segments);

  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunks_to_index;
  chunks_to_index.push_back(std::make_pair(ChunkID{0}, chunk));

  index = std::make_shared<PartialHashIndex>(chunks_to_index, ColumnID{0});
  EXPECT_EQ(index->remove_entries(std::vector<ChunkID>{ChunkID{0}}), 1);
  EXPECT_EQ(index->remove_entries(std::vector<ChunkID>{ChunkID{0}}), 0);

  auto expected_memory_usage = size_t{0};
  // + indexed ColumnID
  expected_memory_usage += sizeof(ColumnID);
  // + data access mutex
  expected_memory_usage += sizeof(std::shared_mutex);
  // + pointer to BaseParialHashIndexImpl
  expected_memory_usage += sizeof(std::unique_ptr<BasePartialHashIndexImpl>);
  // +  ChunkIDs set
  expected_memory_usage += sizeof(std::unordered_set<ChunkID>);
  // +  number of indexed chunks * ChunkID
  expected_memory_usage += 0 * sizeof(ChunkID);
  // + map size
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * tuple size
  expected_memory_usage += 0 * sizeof(std::pair<pmr_string, std::vector<RowID>>);
  // + number of different non-NULL values * vector size
  expected_memory_usage += 0 * sizeof(std::vector<RowID>);
  // + number of non-NULL values * RowID
  expected_memory_usage += 0 * sizeof(RowID);
  // + vector size NULL values (index NULL positions)
  expected_memory_usage += sizeof(tsl::sparse_map<pmr_string, std::vector<RowID>>);
  // + vector size when NULL values are indexed
  expected_memory_usage += 0 * sizeof(std::vector<RowID>);
  // + number of NULL values * RowID
  expected_memory_usage += 0 * sizeof(RowID);

  EXPECT_EQ(index->estimate_memory_usage(), expected_memory_usage);
}

}  // namespace hyrise
