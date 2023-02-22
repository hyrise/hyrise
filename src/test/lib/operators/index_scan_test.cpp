#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

class OperatorsIndexScanTest : public BaseTest {
 protected:
  void SetUp() override {
    auto int_int_7 = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
    auto int_int_5 = load_table("resources/test_data/tbl/int_int_shuffled_2.tbl", ChunkOffset{5});

    ChunkEncoder::encode_all_chunks(int_int_7);
    ChunkEncoder::encode_all_chunks(int_int_5);

    _chunk_ids = std::vector<ChunkID>(int_int_7->chunk_count());
    std::iota(_chunk_ids.begin(), _chunk_ids.end(), ChunkID{0});

    _chunk_ids_partly_compressed = std::vector<ChunkID>(int_int_5->chunk_count());
    std::iota(_chunk_ids_partly_compressed.begin(), _chunk_ids_partly_compressed.end(), ChunkID{0});

    _column_ids = std::vector<ColumnID>{ColumnID{0u}};

    int_int_7->create_partial_hash_index(_column_ids[0], _chunk_ids);
    int_int_5->create_partial_hash_index(_column_ids[0], _chunk_ids_partly_compressed);

    _int_int = std::make_shared<TableWrapper>(std::move(int_int_7));
    _int_int->never_clear_output();
    _int_int->execute();

    _int_int_small_chunk = std::make_shared<TableWrapper>(std::move(int_int_5));
    _int_int_small_chunk->never_clear_output();
    _int_int_small_chunk->execute();

    const auto partially_indexed_table = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
    ChunkEncoder::encode_all_chunks(partially_indexed_table);
    const auto second_chunk = partially_indexed_table->get_chunk(ChunkID{1});
    // second_chunk->template create_index<DerivedIndex>(std::vector<ColumnID>{ColumnID{0}});
    Hyrise::get().storage_manager.add_table("index_test_table", partially_indexed_table);
  }

  void ASSERT_COLUMN_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                        std::vector<AllTypeVariant> expected) {
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset) {
        const auto& segment = *chunk->get_segment(column_id);

        const auto found_value = segment[chunk_offset];
        const auto comparator = [found_value](const AllTypeVariant expected_value) {
          // returns equivalency, not equality to simulate std::multiset.
          // multiset cannot be used because it triggers a compiler / lib bug when built in CI
          return !(found_value < expected_value) && !(expected_value < found_value);
        };

        auto search = std::find_if(expected.begin(), expected.end(), comparator);

        ASSERT_TRUE(search != expected.end());
        expected.erase(search);
      }
    }

    ASSERT_EQ(expected.size(), 0u);
  }

  std::shared_ptr<TableWrapper> _int_int;
  std::shared_ptr<TableWrapper> _int_int_small_chunk;
  std::vector<ChunkID> _chunk_ids;
  std::vector<ChunkID> _chunk_ids_partly_compressed;
  std::vector<ColumnID> _column_ids;
};


TEST_F(OperatorsIndexScanTest, SingleColumnScanOnDataTable) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{4}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{9}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {104, 104};
  tests[PredicateCondition::NotEquals] = {100, 102, 106, 108, 110, 112, 100, 102, 106, 108, 110, 112};

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_int_int, this->_column_ids, test.first,
                                            right_values, right_values2);

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(this->_int_int_small_chunk,
                                                        this->_column_ids, test.first, right_values, right_values2);

    scan_small_chunk->execute();

    auto table = scan->get_output();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
    this->ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test.second);
  }
}

TEST_F(OperatorsIndexScanTest, SingleColumnScanValueGreaterThanMaxDictionaryValue) {
  const auto all_rows =
      std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{30}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{34}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_int_int, this->_column_ids, test.first,
                                            right_values, right_values2);

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(this->_int_int_small_chunk,
                                                        this->_column_ids, test.first, right_values, right_values2);

    scan_small_chunk->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
    this->ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test.second);
  }
}

TEST_F(OperatorsIndexScanTest, SingleColumnScanValueLessThanMinDictionaryValue) {
  const auto all_rows =
      std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{-10}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{34}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_int_int, this->_column_ids, test.first,
                                            right_values, right_values2);

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(this->_int_int_small_chunk,
                                                        this->_column_ids, test.first, right_values, right_values2);

    scan_small_chunk->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
    this->ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test.second);
  }
}

TEST_F(OperatorsIndexScanTest, OperatorName) {
  const auto right_values = std::vector<AllTypeVariant>(this->_column_ids.size(), AllTypeVariant{0});

  auto scan = std::make_shared<IndexScan>(this->_int_int, this->_column_ids,
                                          PredicateCondition::GreaterThanEquals, right_values);

  EXPECT_EQ(scan->name(), "IndexScan");
}

}  // namespace hyrise
