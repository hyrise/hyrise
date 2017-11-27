#include <algorithm>
#include <iostream>
#include <numeric>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/index_scan.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

template <typename DerivedIndex>
class OperatorsIndexScanTest : public BaseTest {
 protected:
  void SetUp() override {
    _index_type = get_index_type_of<DerivedIndex>();

    std::shared_ptr<Table> test_even_dict = std::make_shared<Table>(5);
    test_even_dict->add_column("a", DataType::Int);
    test_even_dict->add_column("b", DataType::Int);
    for (int i = 0; i <= 24; i += 2) test_even_dict->append({i, 100 + i});
    DictionaryCompression::compress_table(*test_even_dict);

    _chunk_ids = std::vector<ChunkID>(test_even_dict->chunk_count());
    std::iota(_chunk_ids.begin(), _chunk_ids.end(), ChunkID{0u});

    _column_ids = std::vector<ColumnID>{ColumnID{0u}};

    for (const auto& chunk_id : _chunk_ids) {
      auto& chunk = test_even_dict->get_chunk(chunk_id);
      chunk.create_index<DerivedIndex>(_column_ids);
    }

    _table_wrapper_even_dict = std::make_shared<TableWrapper>(std::move(test_even_dict));
    _table_wrapper_even_dict->execute();
  }

  std::shared_ptr<const Table> to_referencing_table(const std::shared_ptr<const Table>& table) {
    auto table_out = std::make_shared<Table>();

    auto pos_list = std::make_shared<PosList>();
    pos_list->reserve(table->row_count());

    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk.size(); ++chunk_offset) {
        pos_list->push_back(RowID{chunk_id, chunk_offset});
      }
    }

    auto chunk_out = Chunk{};

    for (auto column_id = ColumnID{0u}; column_id < table->column_count(); ++column_id) {
      table_out->add_column_definition(table->column_name(column_id), table->column_type(column_id));

      auto column_out = std::make_shared<ReferenceColumn>(table, column_id, pos_list);
      chunk_out.add_column(column_out);
    }

    table_out->emplace_chunk(std::move(chunk_out));
    return table_out;
  }

  void ASSERT_COLUMN_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                        std::vector<AllTypeVariant> expected) {
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk.size(); ++chunk_offset) {
        const auto& column = *chunk.get_column(column_id);

        const auto found_value = column[chunk_offset];
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

  std::shared_ptr<TableWrapper> _table_wrapper_even_dict;
  std::vector<ChunkID> _chunk_ids;
  std::vector<ColumnID> _column_ids;
  ColumnIndexType _index_type;
};

typedef ::testing::Types<GroupKeyIndex, AdaptiveRadixTreeIndex, CompositeGroupKeyIndex /* add further indices */> DerivedIndices;
TYPED_TEST_CASE(OperatorsIndexScanTest, DerivedIndices);


TYPED_TEST(OperatorsIndexScanTest, DISABLED_DoubleScan) {
  // std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  // auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  // scan_1->execute();

  // auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnID{1}, ScanType::OpLessThan, 457.9);
  // scan_2->execute();

  // EXPECT_TABLE_EQ_UNORDERED(scan_2->get_output(), expected_result);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_EmptyResultScan) {
  // auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThan, 90000);
  // scan_1->execute();

  // for (auto i = ChunkID{0}; i < scan_1->get_output()->chunk_count(); i++)
  //   EXPECT_EQ(scan_1->get_output()->get_chunk(i).column_count(), 2u);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_SingleScanReturnsCorrectRowCount) {
  // std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  // auto scan = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  // scan->execute();

  // EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TYPED_TEST(OperatorsIndexScanTest, SingleScanOnDataTable) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  const auto right_values = std::vector<AllTypeVariant>{4};
  const auto right_values2 = std::vector<AllTypeVariant>{9};

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = {104};
  tests[ScanType::OpNotEquals] = {100, 102, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpLessThan] = {100, 102};
  tests[ScanType::OpLessThanEquals] = {100, 102, 104};
  tests[ScanType::OpGreaterThan] = {106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpGreaterThanEquals] = {104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[ScanType::OpBetween] = {104, 106, 108};

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_table_wrapper_even_dict, this->_index_type, this->_chunk_ids, this->_column_ids,
                                            test.first, right_values, right_values2);

    scan->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanOnReferencedDictColumn) {
  // // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  // std::map<ScanType, std::vector<AllTypeVariant>> tests;
  // tests[ScanType::OpEquals] = {104};
  // tests[ScanType::OpNotEquals] = {100, 102, 106};
  // tests[ScanType::OpLessThan] = {100, 102};
  // tests[ScanType::OpLessThanEquals] = {100, 102, 104};
  // tests[ScanType::OpGreaterThan] = {106};
  // tests[ScanType::OpGreaterThanEquals] = {104, 106};
  // tests[ScanType::OpBetween] = {};  // Will throw

  // for (const auto& test : tests) {
  //   auto scan1 = std::make_shared<TableScan>(_table_wrapper_even_dict, ColumnID{1}, ScanType::OpLessThan, 108);
  //   scan1->execute();

  //   auto scan2 = std::make_shared<TableScan>(scan1, ColumnID{0}, test.first, 4, std::optional<AllTypeVariant>(9));

  //   if (test.first == ScanType::OpBetween) {
  //     EXPECT_THROW(scan2->execute(), std::logic_error);
  //     continue;
  //   }

  //   scan2->execute();

  //   ASSERT_COLUMN_EQ(scan2->get_output(), ColumnID{1}, test.second);
  // }
}

TYPED_TEST(OperatorsIndexScanTest, ScanOnDictColumnValueGreaterThanMaxDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{30};
  const auto right_values2 = std::vector<AllTypeVariant>{34};

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = no_rows;
  tests[ScanType::OpNotEquals] = all_rows;
  tests[ScanType::OpLessThan] = all_rows;
  tests[ScanType::OpLessThanEquals] = all_rows;
  tests[ScanType::OpGreaterThan] = no_rows;
  tests[ScanType::OpGreaterThanEquals] = no_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_table_wrapper_even_dict, this->_index_type, this->_chunk_ids,
                                            this->_column_ids, test.first, right_values, right_values2);
    scan->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, ScanOnDictColumnValueLessThanMinDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{-10};
  const auto right_values2 = std::vector<AllTypeVariant>{34};

  std::map<ScanType, std::vector<AllTypeVariant>> tests;
  tests[ScanType::OpEquals] = no_rows;
  tests[ScanType::OpNotEquals] = all_rows;
  tests[ScanType::OpLessThan] = no_rows;
  tests[ScanType::OpLessThanEquals] = no_rows;
  tests[ScanType::OpGreaterThan] = all_rows;
  tests[ScanType::OpGreaterThanEquals] = all_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_table_wrapper_even_dict, this->_index_type, this->_chunk_ids,
                                            this->_column_ids, test.first, right_values, right_values2);
    scan->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanOnIntValueColumnWithFloatColumnWithNullValues) {
  // auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  // auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  // table_wrapper->execute();

  // auto scan =
  //     std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  // scan->execute();

  // const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  // ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanOnReferencedIntValueColumnWithFloatColumnWithNullValues) {
  // auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  // auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  // table_wrapper->execute();

  // auto scan =
  //     std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  // scan->execute();

  // const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  // ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanOnIntDictColumnWithFloatColumnWithNullValues) {
  // auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  // DictionaryCompression::compress_table(*table);

  // auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  // table_wrapper->execute();

  // auto scan =
  //     std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  // scan->execute();

  // const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  // ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanOnReferencedIntDictColumnWithFloatColumnWithNullValues) {
  // auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  // DictionaryCompression::compress_table(*table);

  // auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  // table_wrapper->execute();

  // auto scan =
  //     std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */, ScanType::OpGreaterThan, ColumnID{1} /* "b" */);
  // scan->execute();

  // const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  // ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanOnDictColumnAroundBounds) {
  // // scanning for a value that is around the dictionary's bounds

  // std::map<ScanType, std::vector<AllTypeVariant>> tests;
  // tests[ScanType::OpEquals] = {100};
  // tests[ScanType::OpLessThan] = {};
  // tests[ScanType::OpLessThanEquals] = {100};
  // tests[ScanType::OpGreaterThan] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  // tests[ScanType::OpGreaterThanEquals] = {100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  // tests[ScanType::OpNotEquals] = {102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};

  // for (const auto& test : tests) {
  //   auto scan = std::make_shared<opossum::TableScan>(_table_wrapper_even_dict, ColumnID{0}, test.first, 0,
  //                                                    std::optional<AllTypeVariant>(10));
  //   scan->execute();

  //   ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  // }
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanWithEmptyInput) {
  // auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThan, 12345);
  // scan_1->execute();
  // EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(0));

  // // scan_1 produced an empty result
  // auto scan_2 = std::make_shared<opossum::TableScan>(scan_1, ColumnID{1}, ScanType::OpEquals, 456.7);
  // scan_2->execute();

  // EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(0));
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanOnWideDictionaryColumn) {
  // // 2**8 + 1 values require a data type of 16bit.
  // const auto table_wrapper_dict_16 = get_table_op_with_n_dict_entries((1 << 8) + 1);
  // auto scan_1 = std::make_shared<opossum::TableScan>(table_wrapper_dict_16, ColumnID{0}, ScanType::OpGreaterThan, 200);
  // scan_1->execute();

  // EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(57));

  // // 2**16 + 1 values require a data type of 32bit.
  // const auto table_wrapper_dict_32 = get_table_op_with_n_dict_entries((1 << 16) + 1);
  // auto scan_2 =
  //     std::make_shared<opossum::TableScan>(table_wrapper_dict_32, ColumnID{0}, ScanType::OpGreaterThan, 65500);
  // scan_2->execute();

  // EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(37));
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_OperatorName) {
  // auto scan_1 = std::make_shared<opossum::TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);

  // EXPECT_EQ(scan_1->name(), "TableScan");
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesOnValueColumn) {
  // auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4));
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
  //     {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  // scan_for_null_values(table_wrapper, tests);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesOnDictColumn) {
  // auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  // DictionaryCompression::compress_table(*table);

  // auto table_wrapper = std::make_shared<TableWrapper>(table);
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
  //     {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  // scan_for_null_values(table_wrapper, tests);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesOnValueColumnWithoutNulls) {
  // auto table = load_table("src/test/tables/int_float.tbl", 4);

  // auto table_wrapper = std::make_shared<TableWrapper>(table);
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{{ScanType::OpEquals, {}},
  //                                                                    {ScanType::OpNotEquals, {12345, 123, 1234}}};

  // scan_for_null_values(table_wrapper, tests);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesOnReferencedValueColumnWithoutNulls) {
  // auto table = load_table("src/test/tables/int_float.tbl", 4);

  // auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{{ScanType::OpEquals, {}},
  //                                                                    {ScanType::OpNotEquals, {12345, 123, 1234}}};

  // scan_for_null_values(table_wrapper, tests);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesOnReferencedValueColumn) {
  // auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);

  // auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
  //     {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  // scan_for_null_values(table_wrapper, tests);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesOnReferencedDictColumn) {
  // auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 4);
  // DictionaryCompression::compress_table(*table);

  // auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{
  //     {ScanType::OpEquals, {12, 123}}, {ScanType::OpNotEquals, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  // scan_for_null_values(table_wrapper, tests);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesWithNullRowIDOnReferencedValueColumn) {
  // auto table = create_referencing_table_w_null_row_id(false);

  // auto table_wrapper = std::make_shared<TableWrapper>(table);
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{{ScanType::OpEquals, {123, 1234}},
  //                                                                    {ScanType::OpNotEquals, {12345, NULL_VALUE}}};

  // scan_for_null_values(table_wrapper, tests);
}

TYPED_TEST(OperatorsIndexScanTest, DISABLED_ScanForNullValuesWithNullRowIDOnReferencedDictColumn) {
  // auto table = create_referencing_table_w_null_row_id(true);

  // auto table_wrapper = std::make_shared<TableWrapper>(table);
  // table_wrapper->execute();

  // const auto tests = std::map<ScanType, std::vector<AllTypeVariant>>{{ScanType::OpEquals, {123, 1234}},
  //                                                                    {ScanType::OpNotEquals, {12345, NULL_VALUE}}};

  // scan_for_null_values(table_wrapper, tests);
}

}  // namespace opossum
