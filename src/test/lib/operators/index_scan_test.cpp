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

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

std::shared_ptr<std::vector<ChunkID>> shared_chunk_id_vector(std::vector<ChunkID>&& chunk_vector) {
  return std::make_shared<std::vector<ChunkID>>(chunk_vector);
}

}  // namespace

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

    _column_id = ColumnID{0};

    int_int_7->create_partial_hash_index(_column_id, _chunk_ids);
    int_int_5->create_partial_hash_index(_column_id, _chunk_ids_partly_compressed);

    Hyrise::get().storage_manager.add_table("int_int_7", int_int_7);
    Hyrise::get().storage_manager.add_table("int_int_5", int_int_5);

    _int_int = std::make_shared<GetTable>("int_int_7");
    _int_int->never_clear_output();
    _int_int->execute();

    _int_int_small_chunk = std::make_shared<GetTable>("int_int_5");
    _int_int_small_chunk->never_clear_output();
    _int_int_small_chunk->execute();

    const auto partially_indexed_table = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
    ChunkEncoder::encode_all_chunks(partially_indexed_table);
    const auto second_chunk = partially_indexed_table->get_chunk(ChunkID{1});
    Hyrise::get().storage_manager.add_table("index_test_table", partially_indexed_table);
  }

  void ASSERT_COLUMN_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                        std::vector<AllTypeVariant> expected) {
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);

      const auto chunk_size = chunk->size();
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        const auto& segment = *chunk->get_segment(column_id);

        const auto& found_value = segment[chunk_offset];
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

    ASSERT_EQ(expected.size(), 0);
  }

  std::shared_ptr<GetTable> _int_int;
  std::shared_ptr<GetTable> _int_int_small_chunk;
  std::vector<ChunkID> _chunk_ids;
  std::vector<ChunkID> _chunk_ids_partly_compressed;
  ColumnID _column_id;
};

TEST_F(OperatorsIndexScanTest, SingleColumnScanOnDataTable) {
  const auto right_value = AllTypeVariant{4};

  auto tests = std::vector<std::pair<PredicateCondition, std::vector<AllTypeVariant>>>{
      {PredicateCondition::Equals, {104, 104}},
      {PredicateCondition::NotEquals, {100, 102, 106, 108, 110, 112, 100, 102, 106, 108, 110, 112}}};

  for (const auto& [predicate, test_rows] : tests) {
    auto scan = std::make_shared<IndexScan>(_int_int, _column_id, predicate, right_value);
    scan->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}, ChunkID{1}});

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(_int_int_small_chunk, _column_id, predicate, right_value);
    scan_small_chunk->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}, ChunkID{1}, ChunkID{2}});

    scan_small_chunk->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test_rows);
    ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test_rows);
  }
}

TEST_F(OperatorsIndexScanTest, ScanWithNoChunkIDsIncluded) {
  auto tests = std::vector<std::pair<PredicateCondition, std::vector<AllTypeVariant>>>{
      {PredicateCondition::Equals, {}}, {PredicateCondition::NotEquals, {}}};

  auto scan = std::make_shared<IndexScan>(_int_int, _column_id, PredicateCondition::Equals, AllTypeVariant{17});
  scan->included_chunk_ids = std::make_shared<std::vector<ChunkID>>();
  EXPECT_THROW(scan->execute(), std::logic_error);
}

TEST_F(OperatorsIndexScanTest, SingleColumnScanValueGreaterThanMaxDictionaryValue) {
  const auto right_value = AllTypeVariant{30};

  auto tests = std::vector<std::pair<PredicateCondition, std::vector<AllTypeVariant>>>{
      {PredicateCondition::Equals, {}},
      {PredicateCondition::NotEquals, {100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112}}};

  for (const auto& [predicate, test_rows] : tests) {
    auto scan = std::make_shared<IndexScan>(_int_int, _column_id, predicate, right_value);
    scan->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}, ChunkID{1}});

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(_int_int_small_chunk, _column_id, predicate, right_value);
    scan_small_chunk->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}, ChunkID{1}, ChunkID{2}});

    scan_small_chunk->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test_rows);
    ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test_rows);
  }
}

TEST_F(OperatorsIndexScanTest, SingleColumnScanValueLessThanMinDictionaryValue) {
  const auto right_value = AllTypeVariant{-10};

  auto tests = std::vector<std::pair<PredicateCondition, std::vector<AllTypeVariant>>>{
      {PredicateCondition::Equals, {}},
      {PredicateCondition::NotEquals, {100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112}}};

  for (const auto& [predicate, test_rows] : tests) {
    auto scan = std::make_shared<IndexScan>(_int_int, _column_id, predicate, right_value);
    scan->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}, ChunkID{1}});

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(_int_int_small_chunk, _column_id, predicate, right_value);
    scan_small_chunk->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}, ChunkID{1}, ChunkID{2}});

    scan_small_chunk->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test_rows);
    ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test_rows);
  }
}

TEST_F(OperatorsIndexScanTest, DynamicallyPrunedChunks) {
  auto table = load_table("resources/test_data/tbl/int_string.tbl", ChunkOffset{1});

  ChunkEncoder::encode_all_chunks(table);

  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), ChunkID{0});
  table->create_partial_hash_index(ColumnID{0}, chunk_ids);

  Hyrise::get().storage_manager.add_table("table", table);

  const auto pruned_chunk_id_tests = std::vector<std::pair<std::vector<ChunkID>, std::vector<AllTypeVariant>>>{
      // Prunes chunks with values 6, 10, 12, and 14.
      {{ChunkID{2}, ChunkID{4}, ChunkID{6}, ChunkID{7}}, {2, 4, 8, 19}},
      // Prunes all chunks (also the ones that are included in the scan). This can happen with dynamic pruning, where
      // chunks are pruned (and added to list of pruned chunks of the GetTable operator) at runtime.
      {{ChunkID{0}, ChunkID{1}, ChunkID{2}, ChunkID{3}, ChunkID{4}, ChunkID{5}, ChunkID{6}, ChunkID{7}, ChunkID{8},
        ChunkID{9}, ChunkID{10}, ChunkID{11}},
       {}}};

  for (const auto& [pruned_chunk_ids, result] : pruned_chunk_id_tests) {
    auto get_table = std::make_shared<GetTable>("table", pruned_chunk_ids, std::vector<ColumnID>{});
    get_table->never_clear_output();
    get_table->execute();

    auto index_scan =
        std::make_shared<IndexScan>(get_table, ColumnID{0}, PredicateCondition::NotEquals, AllTypeVariant{-17});
    // We include chunks with values 2, 4, and 6.
    index_scan->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}, ChunkID{1}, ChunkID{2}, ChunkID{5}});

    index_scan->never_clear_output();
    index_scan->execute();

    ASSERT_COLUMN_EQ(index_scan->get_output(), ColumnID{0}, result);
  }

  Hyrise::get().storage_manager.drop_table("table");
}

TEST_F(OperatorsIndexScanTest, OperatorName) {
  const auto scan =
      std::make_shared<IndexScan>(_int_int, _column_id, PredicateCondition::GreaterThanEquals, AllTypeVariant{0});
  EXPECT_EQ(scan->name(), "IndexScan");
}

TEST_F(OperatorsIndexScanTest, DeepCopyRetainsIncludedChunks) {
  const auto index_scan =
      std::make_shared<IndexScan>(_int_int, _column_id, PredicateCondition::GreaterThanEquals, AllTypeVariant{0});
  index_scan->included_chunk_ids = shared_chunk_id_vector({ChunkID{0}});
  const auto new_index_scan = std::dynamic_pointer_cast<IndexScan>(index_scan->deep_copy());
  EXPECT_EQ(*index_scan->included_chunk_ids, *new_index_scan->included_chunk_ids);
  EXPECT_EQ(index_scan->included_chunk_ids->data(),
            new_index_scan->included_chunk_ids->data());  // Should be the same object.
}

}  // namespace hyrise
