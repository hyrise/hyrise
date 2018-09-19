#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsTableScanTest : public BaseTest, public ::testing::WithParamInterface<EncodingType> {
 protected:
  void SetUp() override {
    _encoding_type = GetParam();

    auto int_int_7 = load_table("src/test/tables/int_int_shuffled.tbl", 7);
    auto int_int_5 = load_table("src/test/tables/int_int_shuffled_2.tbl", 5);

    ChunkEncoder::encode_chunks(int_int_7, {ChunkID{0}, ChunkID{1}}, {_encoding_type});
    // partly compressed table
    ChunkEncoder::encode_chunks(int_int_5, {ChunkID{0}, ChunkID{1}}, {_encoding_type});

    _int_int_compressed = std::make_shared<TableWrapper>(std::move(int_int_7));
    _int_int_compressed->execute();
    _int_int_partly_compressed = std::make_shared<TableWrapper>(std::move(int_int_5));
    _int_int_partly_compressed->execute();
  }

  std::shared_ptr<TableWrapper> get_table_op() {
    auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_table_op_null() {
    auto table_wrapper_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 2));
    table_wrapper_null->execute();
    return table_wrapper_null;
  }

  std::shared_ptr<TableWrapper> get_table_op_filtered() {
    TableColumnDefinitions table_column_definitions;
    table_column_definitions.emplace_back("a", DataType::Int);
    table_column_definitions.emplace_back("b", DataType::Int);

    std::shared_ptr<Table> table = std::make_shared<Table>(table_column_definitions, TableType::References, 5);

    const auto test_table_part_compressed = _int_int_partly_compressed->get_output();

    auto pos_list = std::make_shared<PosList>();
    pos_list->emplace_back(RowID{ChunkID{2}, 0});
    pos_list->emplace_back(RowID{ChunkID{1}, 1});
    pos_list->emplace_back(RowID{ChunkID{1}, 3});
    pos_list->emplace_back(RowID{ChunkID{0}, 2});
    pos_list->emplace_back(RowID{ChunkID{2}, 2});
    pos_list->emplace_back(RowID{ChunkID{0}, 0});
    pos_list->emplace_back(RowID{ChunkID{0}, 4});

    auto segment_a = std::make_shared<ReferenceSegment>(test_table_part_compressed, ColumnID{0}, pos_list);
    auto segment_b = std::make_shared<ReferenceSegment>(test_table_part_compressed, ColumnID{1}, pos_list);

    Segments segments({segment_a, segment_b});

    table->append_chunk(segments);
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_table_op_with_n_dict_entries(const int num_entries) {
    // Set up dictionary encoded table with a dictionary consisting of num_entries entries.
    TableColumnDefinitions table_column_definitions;
    table_column_definitions.emplace_back("a", DataType::Int);

    std::shared_ptr<Table> table = std::make_shared<Table>(table_column_definitions, TableType::Data);

    for (int i = 0; i <= num_entries; i++) {
      table->append({i});
    }

    ChunkEncoder::encode_chunks(table, {ChunkID{0}}, {_encoding_type});

    auto table_wrapper = std::make_shared<opossum::TableWrapper>(std::move(table));
    table_wrapper->execute();
    return table_wrapper;
  }

  std::shared_ptr<const Table> to_referencing_table(const std::shared_ptr<const Table>& table) {
    auto pos_list = std::make_shared<PosList>();
    pos_list->reserve(table->row_count());

    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset) {
        pos_list->push_back(RowID{chunk_id, chunk_offset});
      }
    }

    Segments segments;
    TableColumnDefinitions column_definitions;

    for (auto column_id = ColumnID{0u}; column_id < table->column_count(); ++column_id) {
      column_definitions.emplace_back(table->column_name(column_id), table->column_data_type(column_id));

      auto segment_out = std::make_shared<ReferenceSegment>(table, column_id, pos_list);
      segments.push_back(segment_out);
    }

    auto table_out = std::make_shared<Table>(column_definitions, TableType::References);

    table_out->append_chunk(segments);

    return table_out;
  }

  std::shared_ptr<const Table> create_referencing_table_w_null_row_id(const bool references_dict_segment) {
    const auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);

    if (references_dict_segment) {
      ChunkEncoder::encode_all_chunks(table, _encoding_type);
    }

    auto pos_list_a = std::make_shared<PosList>(
        PosList{RowID{ChunkID{0u}, 1u}, RowID{ChunkID{1u}, 0u}, RowID{ChunkID{0u}, 2u}, RowID{ChunkID{0u}, 3u}});
    auto ref_segment_a = std::make_shared<ReferenceSegment>(table, ColumnID{0u}, pos_list_a);

    auto pos_list_b = std::make_shared<PosList>(
        PosList{NULL_ROW_ID, RowID{ChunkID{0u}, 0u}, RowID{ChunkID{1u}, 2u}, RowID{ChunkID{0u}, 1u}});
    auto ref_segment_b = std::make_shared<ReferenceSegment>(table, ColumnID{1u}, pos_list_b);

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, true);
    column_definitions.emplace_back("b", DataType::Int, true);
    auto ref_table = std::make_shared<Table>(column_definitions, TableType::References);

    Segments segments({ref_segment_a, ref_segment_b});

    ref_table->append_chunk(segments);

    return ref_table;
  }

  void scan_for_null_values(const std::shared_ptr<AbstractOperator> in,
                            const std::map<PredicateCondition, std::vector<AllTypeVariant>>& tests) {
    for (const auto& test : tests) {
      const auto predicate_condition = test.first;
      const auto& expected = test.second;

      auto scan = std::make_shared<TableScan>(
          in, OperatorScanPredicate{ColumnID{1} /* "b" */, predicate_condition, NULL_VALUE});
      scan->execute();

      const auto expected_result = std::vector<AllTypeVariant>{{12, 123}};
      ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
    }
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

 protected:
  EncodingType _encoding_type;
  std::shared_ptr<TableWrapper> _int_int_compressed;
  std::shared_ptr<TableWrapper> _int_int_partly_compressed;
};

auto formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

INSTANTIATE_TEST_CASE_P(EncodingTypes, OperatorsTableScanTest,
                        ::testing::Values(EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::RunLength,
                                          EncodingType::FrameOfReference),
                        formatter);

TEST_P(OperatorsTableScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  auto scan_1 = std::make_shared<TableScan>(
      get_table_op(), OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234});
  scan_1->execute();

  auto scan_2 =
      std::make_shared<TableScan>(scan_1, OperatorScanPredicate{ColumnID{1}, PredicateCondition::LessThan, 457.9});
  scan_2->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan_2->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, EmptyResultScan) {
  auto scan_1 = std::make_shared<TableScan>(get_table_op(),
                                            OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThan, 90000});
  scan_1->execute();

  for (auto i = ChunkID{0}; i < scan_1->get_output()->chunk_count(); i++)
    EXPECT_EQ(scan_1->get_output()->get_chunk(i)->column_count(), 2u);
}

TEST_P(OperatorsTableScanTest, SingleScanReturnsCorrectRowCount) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);

  auto scan = std::make_shared<TableScan>(
      get_table_op(), OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234});
  scan->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, ScanOnCompressedSegments) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {106, 106};
  tests[PredicateCondition::NotEquals] = {100, 102, 104, 108, 110, 112, 100, 102, 104, 108, 110, 112};
  tests[PredicateCondition::LessThan] = {100, 102, 104, 100, 102, 104};
  tests[PredicateCondition::LessThanEquals] = {100, 102, 104, 106, 100, 102, 104, 106};
  tests[PredicateCondition::GreaterThan] = {108, 110, 112, 108, 110, 112};
  tests[PredicateCondition::GreaterThanEquals] = {106, 108, 110, 112, 106, 108, 110, 112};
  tests[PredicateCondition::Between] = {};  // Will throw
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};

  for (const auto& test : tests) {
    auto scan_int = std::make_shared<TableScan>(_int_int_compressed, OperatorScanPredicate{ColumnID{0}, test.first, 6});
    auto scan_int_partly =
        std::make_shared<TableScan>(_int_int_partly_compressed, OperatorScanPredicate{ColumnID{0}, test.first, 6});

    if (test.first == PredicateCondition::Between) {
      EXPECT_THROW(scan_int->execute(), std::logic_error);
      EXPECT_THROW(scan_int_partly->execute(), std::logic_error);
      continue;
    }

    scan_int->execute();

    ASSERT_COLUMN_EQ(scan_int->get_output(), ColumnID{1}, test.second);

    scan_int_partly->execute();
    ASSERT_COLUMN_EQ(scan_int_partly->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedCompressedSegments) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {104, 104};
  tests[PredicateCondition::NotEquals] = {100, 102, 106, 100, 102, 106};
  tests[PredicateCondition::LessThan] = {100, 102, 100, 102};
  tests[PredicateCondition::LessThanEquals] = {100, 102, 104, 100, 102, 104};
  tests[PredicateCondition::GreaterThan] = {106, 106};
  tests[PredicateCondition::GreaterThanEquals] = {104, 106, 104, 106};
  tests[PredicateCondition::Between] = {};  // Will throw
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106, 100, 102, 104, 106};

  for (const auto& test : tests) {
    auto scan1 = std::make_shared<TableScan>(_int_int_compressed,
                                             OperatorScanPredicate{ColumnID{1}, PredicateCondition::LessThan, 108});
    scan1->execute();

    auto scan_partly1 = std::make_shared<TableScan>(
        _int_int_partly_compressed, OperatorScanPredicate{ColumnID{1}, PredicateCondition::LessThan, 108});
    scan_partly1->execute();

    auto scan2 = std::make_shared<TableScan>(scan1, OperatorScanPredicate{ColumnID{0}, test.first, 4});
    auto scan_partly2 = std::make_shared<TableScan>(scan_partly1, OperatorScanPredicate{ColumnID{0}, test.first, 4});

    if (test.first == PredicateCondition::Between) {
      EXPECT_THROW(scan2->execute(), std::logic_error);
      EXPECT_THROW(scan_partly2->execute(), std::logic_error);
      continue;
    }

    scan2->execute();
    scan_partly2->execute();

    ASSERT_COLUMN_EQ(scan2->get_output(), ColumnID{1}, test.second);
    ASSERT_COLUMN_EQ(scan_partly2->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanWeirdPosList) {
  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {110, 110};
  tests[PredicateCondition::NotEquals] = {100, 102, 106, 108, 112};
  tests[PredicateCondition::LessThan] = {100, 102, 106, 108};
  tests[PredicateCondition::LessThanEquals] = {100, 102, 106, 108, 110, 110};
  tests[PredicateCondition::GreaterThan] = {112};
  tests[PredicateCondition::GreaterThanEquals] = {110, 110, 112};
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 106, 108, 110, 110, 112};

  auto table_wrapper = get_table_op_filtered();

  for (const auto& test : tests) {
    auto scan_partly = std::make_shared<TableScan>(table_wrapper, OperatorScanPredicate{ColumnID{0}, test.first, 10});
    scan_partly->execute();

    ASSERT_COLUMN_EQ(scan_partly->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnCompressedSegmentsValueGreaterThanMaxDictionaryValue) {
  const auto all_rows =
      std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};
  const auto no_rows = std::vector<AllTypeVariant>{};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = all_rows;
  tests[PredicateCondition::LessThanEquals] = all_rows;
  tests[PredicateCondition::GreaterThan] = no_rows;
  tests[PredicateCondition::GreaterThanEquals] = no_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(_int_int_compressed, OperatorScanPredicate{ColumnID{0}, test.first, 30});
    scan->execute();

    auto scan_partly =
        std::make_shared<TableScan>(_int_int_partly_compressed, OperatorScanPredicate{ColumnID{0}, test.first, 30});
    scan_partly->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
    ASSERT_COLUMN_EQ(scan_partly->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnCompressedSegmentsValueLessThanMinDictionaryValue) {
  const auto all_rows =
      std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};
  const auto no_rows = std::vector<AllTypeVariant>{};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = no_rows;
  tests[PredicateCondition::LessThanEquals] = no_rows;
  tests[PredicateCondition::GreaterThan] = all_rows;
  tests[PredicateCondition::GreaterThanEquals] = all_rows;

  for (const auto& test : tests) {
    auto scan =
        std::make_shared<TableScan>(_int_int_compressed, OperatorScanPredicate{ColumnID{0} /* "a" */, test.first, -10});
    scan->execute();

    auto scan_partly = std::make_shared<TableScan>(_int_int_partly_compressed,
                                                   OperatorScanPredicate{ColumnID{0} /* "a" */, test.first, -10});
    scan_partly->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
    ASSERT_COLUMN_EQ(scan_partly->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnIntValueSegmentWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(
      table_wrapper,
      OperatorScanPredicate{ColumnID{0} /* "a" */, PredicateCondition::GreaterThan, ColumnID{1} /* "b" */});
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedIntValueSegmentWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(
      table_wrapper,
      OperatorScanPredicate{ColumnID{0} /* "a" */, PredicateCondition::GreaterThan, ColumnID{1} /* "b" */});
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnIntCompressedSegmentsWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, _encoding_type);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(
      table_wrapper,
      OperatorScanPredicate{ColumnID{0} /* "a" */, PredicateCondition::GreaterThan, ColumnID{1} /* "b" */});
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedIntCompressedSegmentsWithFloatColumnWithNullValues) {
  auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, _encoding_type);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(
      table_wrapper,
      OperatorScanPredicate{ColumnID{0} /* "a" */, PredicateCondition::GreaterThan, ColumnID{1} /* "b" */});
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnCompressedSegmentsAroundBounds) {
  // scanning for a value that is around the dictionary's bounds

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {100, 100};
  tests[PredicateCondition::LessThan] = {};
  tests[PredicateCondition::LessThanEquals] = {100, 100};
  tests[PredicateCondition::GreaterThan] = {102, 104, 106, 108, 110, 112, 102, 104, 106, 108, 110, 112};
  tests[PredicateCondition::GreaterThanEquals] = {100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};
  tests[PredicateCondition::NotEquals] = {102, 104, 106, 108, 110, 112, 102, 104, 106, 108, 110, 112};
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};

  for (const auto& test : tests) {
    auto scan = std::make_shared<TableScan>(_int_int_compressed, OperatorScanPredicate{ColumnID{0}, test.first, 0});
    scan->execute();

    auto scan_partly =
        std::make_shared<TableScan>(_int_int_partly_compressed, OperatorScanPredicate{ColumnID{0}, test.first, 0});
    scan_partly->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
    ASSERT_COLUMN_EQ(scan_partly->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanWithEmptyInput) {
  auto scan_1 = std::make_shared<TableScan>(get_table_op(),
                                            OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThan, 12345});
  scan_1->execute();
  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(0));

  // scan_1 produced an empty result
  auto scan_2 =
      std::make_shared<TableScan>(scan_1, OperatorScanPredicate{ColumnID{1}, PredicateCondition::Equals, 456.7});
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(0));
}

TEST_P(OperatorsTableScanTest, ScanOnWideDictionarySegment) {
  // 2**8 + 1 values require a data type of 16bit.
  const auto table_wrapper_dict_16 = get_table_op_with_n_dict_entries((1 << 8) + 1);
  auto scan_1 = std::make_shared<TableScan>(table_wrapper_dict_16,
                                            OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThan, 200});
  scan_1->execute();

  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(57));

  // 2**16 + 1 values require a data type of 32bit.
  const auto table_wrapper_dict_32 = get_table_op_with_n_dict_entries((1 << 16) + 1);
  auto scan_2 = std::make_shared<TableScan>(table_wrapper_dict_32,
                                            OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThan, 65500});
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(37));
}

TEST_P(OperatorsTableScanTest, OperatorName) {
  auto scan_1 = std::make_shared<TableScan>(
      get_table_op(), OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234});

  EXPECT_EQ(scan_1->name(), "TableScan");
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnValueSegment) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnCompressedSegments) {
  auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, _encoding_type);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnValueSegmentWithoutNulls) {
  auto table = load_table("src/test/tables/int_float.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {}}, {PredicateCondition::IsNotNull, {12345, 123, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedValueSegmentWithoutNulls) {
  auto table = load_table("src/test/tables/int_float.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {}}, {PredicateCondition::IsNotNull, {12345, 123, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedValueSegment) {
  auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedCompressedSegments) {
  auto table = load_table("src/test/tables/int_int_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, _encoding_type);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesWithNullRowIDOnReferencedValueSegment) {
  auto table = create_referencing_table_w_null_row_id(false);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {123, 1234}}, {PredicateCondition::IsNotNull, {12345, NULL_VALUE}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesWithNullRowIDOnReferencedCompressedSegments) {
  auto table = create_referencing_table_w_null_row_id(true);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {123, 1234}}, {PredicateCondition::IsNotNull, {12345, NULL_VALUE}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, NullSemantics) {
  const auto predicate_conditions = std::vector<PredicateCondition>(
      {PredicateCondition::Equals, PredicateCondition::NotEquals, PredicateCondition::LessThan,
       PredicateCondition::LessThanEquals, PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals});

  for (auto predicate_condition : predicate_conditions) {
    auto scan = std::make_shared<TableScan>(get_table_op_null(),
                                            OperatorScanPredicate{ColumnID{0}, predicate_condition, NULL_VALUE});
    scan->execute();

    EXPECT_EQ(scan->get_output()->row_count(), 0u);

    for (auto i = ChunkID{0}; i < scan->get_output()->chunk_count(); i++) {
      EXPECT_EQ(scan->get_output()->get_chunk(i)->column_count(), 2u);
    }
  }
}

TEST_P(OperatorsTableScanTest, ScanWithExcludedFirstChunk) {
  const auto expected = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 102, 104};

  auto scan = std::make_shared<TableScan>(_int_int_partly_compressed,
                                          OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThanEquals, 0});
  scan->set_excluded_chunk_ids({ChunkID{0u}});
  scan->execute();

  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, expected);
}

TEST_P(OperatorsTableScanTest, SetParameters) {
  const auto parameters = std::unordered_map<ParameterID, AllTypeVariant>{{ParameterID{3}, AllTypeVariant{5}},
                                                                          {ParameterID{2}, AllTypeVariant{6}}};

  const auto scan_a = std::make_shared<TableScan>(
      _int_int_compressed, OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThanEquals, 4});
  scan_a->set_parameters(parameters);
  EXPECT_EQ(scan_a->predicate().column_id, ColumnID{0});
  EXPECT_EQ(scan_a->predicate().value, AllParameterVariant{4});

  const auto scan_b = std::make_shared<TableScan>(
      _int_int_compressed, OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThanEquals, ParameterID{2}});
  scan_b->set_parameters(parameters);
  EXPECT_EQ(scan_b->predicate().column_id, ColumnID{0});
  EXPECT_EQ(scan_b->predicate().value, AllParameterVariant{6});

  const auto scan_c = std::make_shared<TableScan>(
      _int_int_compressed, OperatorScanPredicate{ColumnID{0}, PredicateCondition::GreaterThanEquals, ParameterID{4}});
  scan_c->set_parameters(parameters);
  EXPECT_EQ(scan_c->predicate().column_id, ColumnID{0});
  EXPECT_EQ(scan_c->predicate().value, AllParameterVariant{ParameterID{4}});
}

}  // namespace opossum
