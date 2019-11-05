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

#include "expression/expression_functional.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/limit.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_scan/column_between_table_scan_impl.hpp"
#include "operators/table_scan/column_is_null_table_scan_impl.hpp"
#include "operators/table_scan/column_like_table_scan_impl.hpp"
#include "operators/table_scan/column_vs_column_table_scan_impl.hpp"
#include "operators/table_scan/column_vs_value_table_scan_impl.hpp"
#include "operators/table_scan/expression_evaluator_table_scan_impl.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorsTableScanTest : public BaseTest, public ::testing::WithParamInterface<EncodingType> {
 protected:
  void SetUp() override {
    _encoding_type = GetParam();

    auto int_int_7 = load_table("resources/test_data/tbl/int_int_shuffled.tbl", 7);
    auto int_int_5 = load_table("resources/test_data/tbl/int_int_shuffled_2.tbl", 5);

    ChunkEncoder::encode_chunks(int_int_7, {ChunkID{0}, ChunkID{1}}, {_encoding_type});
    // partly compressed table
    ChunkEncoder::encode_chunks(int_int_5, {ChunkID{0}, ChunkID{1}}, {_encoding_type});

    _int_int_compressed = std::make_shared<TableWrapper>(std::move(int_int_7));
    _int_int_compressed->execute();
    _int_int_partly_compressed = std::make_shared<TableWrapper>(std::move(int_int_5));
    _int_int_partly_compressed->execute();
  }

  std::shared_ptr<TableWrapper> load_and_encode_table(
      const std::string& path, const ChunkOffset chunk_size = 2,
      const std::optional<std::pair<ColumnID, OrderByMode>> ordered_by = std::nullopt) {
    const auto table = load_table(path, chunk_size);

    auto chunk_encoding_spec = ChunkEncodingSpec{};
    for (const auto& column_definition : table->column_definitions()) {
      if (encoding_supports_data_type(_encoding_type, column_definition.data_type)) {
        chunk_encoding_spec.emplace_back(_encoding_type);
      } else {
        chunk_encoding_spec.emplace_back(EncodingType::Unencoded);
      }
    }

    ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);

    if (ordered_by) {
      const auto chunk_count = table->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = table->get_chunk(chunk_id);
        if (!chunk) continue;

        chunk->set_ordered_by(ordered_by.value());
      }
    }

    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    return table_wrapper;
  }

  std::shared_ptr<TableWrapper> get_int_float_op() {
    return load_and_encode_table("resources/test_data/tbl/int_float.tbl");
  }

  std::shared_ptr<TableWrapper> get_int_sorted_op() {
    return load_and_encode_table("resources/test_data/tbl/int_sorted.tbl", 10,
                                 std::make_optional(std::make_pair(ColumnID(0), OrderByMode::Ascending)));
  }

  std::shared_ptr<TableWrapper> get_int_string_op() {
    return load_and_encode_table("resources/test_data/tbl/int_string.tbl");
  }

  std::shared_ptr<TableWrapper> get_int_float_with_null_op(const ChunkOffset chunk_size = 2) {
    return load_and_encode_table("resources/test_data/tbl/int_float_with_null.tbl", chunk_size);
  }

  std::shared_ptr<TableWrapper> get_table_op_filtered() {
    TableColumnDefinitions table_column_definitions;
    table_column_definitions.emplace_back("a", DataType::Int, false);
    table_column_definitions.emplace_back("b", DataType::Int, false);

    std::shared_ptr<Table> table = std::make_shared<Table>(table_column_definitions, TableType::References);

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
    table_column_definitions.emplace_back("a", DataType::Int, false);

    std::shared_ptr<Table> table = std::make_shared<Table>(table_column_definitions, TableType::Data);

    for (int i = 0; i <= num_entries; i++) {
      table->append({i});
    }

    table->get_chunk(static_cast<ChunkID>(ChunkID{0}))->finalize();

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
      column_definitions.emplace_back(table->column_name(column_id), table->column_data_type(column_id),
                                      table->column_is_nullable(column_id));

      auto segment_out = std::make_shared<ReferenceSegment>(table, column_id, pos_list);
      segments.push_back(segment_out);
    }

    auto table_out = std::make_shared<Table>(column_definitions, TableType::References);

    table_out->append_chunk(segments);

    return table_out;
  }

  std::shared_ptr<const Table> create_referencing_table_w_null_row_id(const bool references_dict_segment) {
    const auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);

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

      const auto column = get_column_expression(in, ColumnID{1});

      auto scan = create_table_scan(in, ColumnID{1}, predicate_condition, NULL_VALUE);
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

        ASSERT_TRUE(search != expected.end()) << found_value << " not found";
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

INSTANTIATE_TEST_SUITE_P(EncodingTypes, OperatorsTableScanTest,
                         ::testing::Values(EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::RunLength,
                                           EncodingType::FrameOfReference),
                         formatter);

TEST_P(OperatorsTableScanTest, DoubleScan) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered.tbl", 2);

  auto scan_1 = create_table_scan(get_int_float_op(), ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan_1->execute();

  auto scan_2 = create_table_scan(scan_1, ColumnID{1}, PredicateCondition::LessThan, 457.9);
  scan_2->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan_2->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, EmptyResultScan) {
  auto scan_1 = create_table_scan(get_int_float_op(), ColumnID{0}, PredicateCondition::GreaterThan, 90000);
  scan_1->execute();

  for (auto i = ChunkID{0}; i < scan_1->get_output()->chunk_count(); i++)
    EXPECT_EQ(scan_1->get_output()->get_chunk(i)->column_count(), 2u);
}

TEST_P(OperatorsTableScanTest, SingleScan) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", 1);

  auto scan = create_table_scan(get_int_float_op(), ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  scan->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, SingleScanWithSortedSegmentEquals) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_sorted_filtered.tbl", 1);

  auto scan = create_table_scan(get_int_sorted_op(), ColumnID{0}, PredicateCondition::Equals, 2);
  scan->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, SingleScanWithSortedSegmentNotEquals) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_sorted_filtered2.tbl", 1);

  auto scan = create_table_scan(get_int_sorted_op(), ColumnID{0}, PredicateCondition::NotEquals, 2);
  scan->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, SingleScanWithSubquery) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", 1);

  const auto subquery_pqp =
      std::make_shared<Limit>(std::make_shared<Projection>(get_int_string_op(), expression_vector(to_expression(1234))),
                              to_expression(int64_t{1}));

  auto scan = std::make_shared<TableScan>(get_int_float_op(),
                                          greater_than_equals_(pqp_column_(ColumnID{0}, DataType::Int, false, "a"),
                                                               pqp_subquery_(subquery_pqp, DataType::Int, false)));
  scan->execute();
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(scan->create_impl().get()));
  EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
}

TEST_P(OperatorsTableScanTest, BetweenScanWithSubquery) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", 1);

  const auto subquery_pqp =
      std::make_shared<Limit>(std::make_shared<Projection>(get_int_string_op(), expression_vector(to_expression(1234))),
                              to_expression(int64_t{1}));

  {
    auto scan = std::make_shared<TableScan>(
        get_int_float_op(),
        between_inclusive_(pqp_column_(ColumnID{0}, DataType::Int, false, "a"),
                           pqp_subquery_(subquery_pqp, DataType::Int, false), to_expression(int{12345})));
    scan->execute();
    EXPECT_TRUE(dynamic_cast<ColumnBetweenTableScanImpl*>(scan->create_impl().get()));
    EXPECT_TABLE_EQ_UNORDERED(scan->get_output(), expected_result);
  }
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
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};

  for (const auto& test : tests) {
    auto scan_int = create_table_scan(_int_int_compressed, ColumnID{0}, test.first, 6);
    auto scan_int_partly = create_table_scan(_int_int_partly_compressed, ColumnID{0}, test.first, 6);

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
  tests[PredicateCondition::IsNull] = {};
  tests[PredicateCondition::IsNotNull] = {100, 102, 104, 106, 100, 102, 104, 106};

  for (const auto& test : tests) {
    auto scan1 = create_table_scan(_int_int_compressed, ColumnID{1}, PredicateCondition::LessThan, 108);
    scan1->execute();

    auto scan_partly1 = create_table_scan(_int_int_partly_compressed, ColumnID{1}, PredicateCondition::LessThan, 108);
    scan_partly1->execute();

    auto scan2 = create_table_scan(scan1, ColumnID{0}, test.first, 4);
    auto scan_partly2 = create_table_scan(scan_partly1, ColumnID{0}, test.first, 4);

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
    auto scan_partly = create_table_scan(table_wrapper, ColumnID{0}, test.first, 10);
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
    auto scan = create_table_scan(_int_int_compressed, ColumnID{0}, test.first, 30);
    scan->execute();

    auto scan_partly = create_table_scan(_int_int_partly_compressed, ColumnID{0}, test.first, 30);
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
    auto scan = create_table_scan(_int_int_compressed, ColumnID{0} /* "a" */, test.first, -10);
    scan->execute();

    auto scan_partly = create_table_scan(_int_int_partly_compressed, ColumnID{0} /* "a" */, test.first, -10);
    scan_partly->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
    ASSERT_COLUMN_EQ(scan_partly->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanOnIntValueSegmentWithFloatColumnWithNullValues) {
  auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  const auto predicate = greater_than_(get_column_expression(table_wrapper, ColumnID{0}),
                                       get_column_expression(table_wrapper, ColumnID{1}));
  auto scan = std::make_shared<TableScan>(table_wrapper, predicate);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedIntValueSegmentWithFloatColumnWithNullValues) {
  auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto predicate = greater_than_(get_column_expression(table_wrapper, ColumnID{0}),
                                       get_column_expression(table_wrapper, ColumnID{1}));
  auto scan = std::make_shared<TableScan>(table_wrapper, predicate);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnIntCompressedSegmentsWithFloatColumnWithNullValues) {
  auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, _encoding_type);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  const auto predicate = greater_than_(get_column_expression(table_wrapper, ColumnID{0}),
                                       get_column_expression(table_wrapper, ColumnID{1}));
  auto scan = std::make_shared<TableScan>(table_wrapper, predicate);
  scan->execute();

  const auto expected = std::vector<AllTypeVariant>{12345, 1234, 12345, 1234};
  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{0u}, expected);
}

TEST_P(OperatorsTableScanTest, ScanOnReferencedIntCompressedSegmentsWithFloatColumnWithNullValues) {
  auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, _encoding_type);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto predicate = greater_than_(get_column_expression(table_wrapper, ColumnID{0}),
                                       get_column_expression(table_wrapper, ColumnID{1}));
  auto scan = std::make_shared<TableScan>(table_wrapper, predicate);
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
    auto scan = create_table_scan(_int_int_compressed, ColumnID{0}, test.first, 0);
    scan->execute();

    auto scan_partly = create_table_scan(_int_int_partly_compressed, ColumnID{0}, test.first, 0);
    scan_partly->execute();

    ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
    ASSERT_COLUMN_EQ(scan_partly->get_output(), ColumnID{1}, test.second);
  }
}

TEST_P(OperatorsTableScanTest, ScanWithEmptyInput) {
  auto scan_1 = std::make_shared<TableScan>(
      get_int_float_op(), greater_than_(get_column_expression(get_int_float_op(), ColumnID{0}), 12345));
  scan_1->execute();
  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(0));

  // scan_1 produced an empty result
  auto scan_2 = std::make_shared<TableScan>(scan_1, equals_(get_column_expression(scan_1, ColumnID{1}), 456.7));
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(0));
}

TEST_P(OperatorsTableScanTest, ScanOnWideDictionarySegment) {
  // 2**8 + 1 values require a data type of 16bit.
  const auto table_wrapper_dict_16 = get_table_op_with_n_dict_entries((1 << 8) + 1);
  auto scan_1 = std::make_shared<TableScan>(
      table_wrapper_dict_16, greater_than_(get_column_expression(table_wrapper_dict_16, ColumnID{0}), 200));
  scan_1->execute();

  EXPECT_EQ(scan_1->get_output()->row_count(), static_cast<size_t>(57));

  // 2**16 + 1 values require a data type of 32bit.
  const auto table_wrapper_dict_32 = get_table_op_with_n_dict_entries((1 << 16) + 1);
  auto scan_2 = std::make_shared<TableScan>(
      table_wrapper_dict_32, greater_than_(get_column_expression(table_wrapper_dict_32, ColumnID{0}), 65500));
  scan_2->execute();

  EXPECT_EQ(scan_2->get_output()->row_count(), static_cast<size_t>(37));
}

TEST_P(OperatorsTableScanTest, OperatorName) {
  auto scan_1 = std::make_shared<TableScan>(
      get_int_float_op(), greater_than_(get_column_expression(get_int_float_op(), ColumnID{0}), 12345));

  EXPECT_EQ(scan_1->name(), "TableScan");
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnValueSegment) {
  auto table_wrapper =
      std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnCompressedSegments) {
  auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);
  ChunkEncoder::encode_all_chunks(table, _encoding_type);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnValueSegmentWithoutNulls) {
  auto table = load_table("resources/test_data/tbl/int_float.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {}}, {PredicateCondition::IsNotNull, {12345, 123, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedValueSegmentWithoutNulls) {
  auto table = load_table("resources/test_data/tbl/int_float.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {}}, {PredicateCondition::IsNotNull, {12345, 123, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedValueSegment) {
  auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);

  auto table_wrapper = std::make_shared<TableWrapper>(to_referencing_table(table));
  table_wrapper->execute();

  const auto tests = std::map<PredicateCondition, std::vector<AllTypeVariant>>{
      {PredicateCondition::IsNull, {12, 123}},
      {PredicateCondition::IsNotNull, {12345, NULL_VALUE, 1234, 12345, 12, 1234}}};

  scan_for_null_values(table_wrapper, tests);
}

TEST_P(OperatorsTableScanTest, ScanForNullValuesOnReferencedCompressedSegments) {
  auto table = load_table("resources/test_data/tbl/int_int_w_null_8_rows.tbl", 4);
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

TEST_P(OperatorsTableScanTest, ComparisonOfIntColumnAndNullValue) {
  const auto predicate_conditions = std::vector<PredicateCondition>(
      {PredicateCondition::Equals, PredicateCondition::NotEquals, PredicateCondition::LessThan,
       PredicateCondition::LessThanEquals, PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals});

  for (auto predicate_condition : predicate_conditions) {
    auto scan = create_table_scan(get_int_float_with_null_op(), ColumnID{0}, predicate_condition, NullValue{});
    scan->execute();

    EXPECT_EQ(scan->get_output()->row_count(), 0u);

    for (auto chunk_id = ChunkID{0}; chunk_id < scan->get_output()->chunk_count(); chunk_id++) {
      EXPECT_EQ(scan->get_output()->get_chunk(chunk_id)->column_count(), 2u);
    }
  }
}

TEST_P(OperatorsTableScanTest, ComparisonOfStringColumnAndNullValue) {
  const auto predicate_conditions = std::vector<PredicateCondition>(
      {PredicateCondition::Equals, PredicateCondition::NotEquals, PredicateCondition::LessThan,
       PredicateCondition::LessThanEquals, PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals});

  for (auto predicate_condition : predicate_conditions) {
    auto scan = create_table_scan(get_int_string_op(), ColumnID{1}, predicate_condition, NullValue{});
    scan->execute();

    EXPECT_EQ(scan->get_output()->row_count(), 0u);
    for (auto chunk_id = ChunkID{0}; chunk_id < scan->get_output()->chunk_count(); chunk_id++) {
      EXPECT_EQ(scan->get_output()->get_chunk(chunk_id)->column_count(), 2u);
    }
  }
}

TEST_P(OperatorsTableScanTest, MatchesAllExcludesNulls) {
  // Scan implementations will potentially optimize the scan if they can detect that all values in the column match the
  // predicate.
  // Test that if the predicate matches all values in the Chunk, rows with NULL values in the scanned column are
  // nevertheless excluded from the result. E.g. [NULL, 1, 2, NULL] < 5 = [1, 2]

  /**
   * Binary Predicates
   */
  auto predicates = std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant, std::vector<AllTypeVariant>>>{
      {ColumnID{0}, PredicateCondition::Equals, 1234, {1234}},  // Matches all in second chunk
      {ColumnID{0}, PredicateCondition::NotEquals, 100, {12345, 123, 1234}},
      {ColumnID{0}, PredicateCondition::LessThan, 15'000, {12345, 123, 1234}},
      {ColumnID{0}, PredicateCondition::LessThanEquals, 15'000, {12345, 123, 1234}},
      {ColumnID{0}, PredicateCondition::GreaterThan, 0, {12345, 123, 1234}},
      {ColumnID{0}, PredicateCondition::GreaterThanEquals, 0, {12345, 123, 1234}}};

  const auto table = get_int_float_with_null_op();
  for (const auto& [column_id, predicate_condition, value, expected_values] : predicates) {
    const auto scan = create_table_scan(table, column_id, predicate_condition, value);
    scan->execute();
    ASSERT_COLUMN_EQ(scan->get_output(), column_id, expected_values);
  }

  /**
   * BETWEEN
   */
  const auto between_scan = create_table_scan(table, ColumnID{0}, PredicateCondition::BetweenInclusive, 0, 15'000);
  between_scan->execute();
  ASSERT_COLUMN_EQ(between_scan->get_output(), ColumnID{0}, {12345, 123, 1234});

  /**
   * IS NULL
   */
  // Second Chunk of Column 1 has only NULL values
  const auto table_with_null_chunk = load_and_encode_table("resources/test_data/tbl/int_int_int_null.tbl");
  const auto is_null_scan =
      create_table_scan(table_with_null_chunk, ColumnID{1}, PredicateCondition::IsNull, NullValue{});
  is_null_scan->execute();
  ASSERT_COLUMN_EQ(is_null_scan->get_output(), ColumnID{0}, {11, 9});
  ASSERT_COLUMN_EQ(is_null_scan->get_output(), ColumnID{1}, {NullValue{}, NullValue{}});

  /**
   * IS NOT NULL
   */
  // First Chunk of Column 0 has no NULL values
  const auto is_not_null_scan = create_table_scan(table, ColumnID{0}, PredicateCondition::IsNotNull, NullValue{});
  is_not_null_scan->execute();
  ASSERT_COLUMN_EQ(is_not_null_scan->get_output(), ColumnID{0}, {12345, 123, 1234});
}

TEST_P(OperatorsTableScanTest, ScanWithExcludedFirstChunk) {
  const auto expected = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 102, 104};

  auto scan = std::make_shared<TableScan>(
      _int_int_partly_compressed,
      greater_than_equals_(get_column_expression(_int_int_partly_compressed, ColumnID{0}), 0));
  scan->excluded_chunk_ids = {ChunkID{0u}};
  scan->execute();

  ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, expected);
}

TEST_P(OperatorsTableScanTest, BinaryScanOnNullable) {
  auto predicates = std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant, std::vector<AllTypeVariant>>>{
      {ColumnID{0}, PredicateCondition::Equals, 1234, {1234}},
      {ColumnID{0}, PredicateCondition::NotEquals, 123, {12345, 1234}},
      {ColumnID{0}, PredicateCondition::GreaterThan, 123, {12345, 1234}},
      {ColumnID{0}, PredicateCondition::GreaterThanEquals, 124, {12345, 1234}},
      {ColumnID{0}, PredicateCondition::LessThan, 1235, {123, 1234}},
      {ColumnID{0}, PredicateCondition::LessThanEquals, 1234, {123, 1234}}};

  const auto table = get_int_float_with_null_op(Chunk::MAX_SIZE);
  for (const auto& [column_id, predicate_condition, value, expected_values] : predicates) {
    const auto scan = create_table_scan(table, column_id, predicate_condition, value);
    scan->execute();
    ASSERT_COLUMN_EQ(scan->get_output(), column_id, expected_values);
  }
}

TEST_P(OperatorsTableScanTest, BinaryScanWithFloatValueOnIntColumn) {
  auto predicates = std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant, std::vector<AllTypeVariant>>>{
      {ColumnID{0}, PredicateCondition::Equals, 1234.0f, {1234}},
      {ColumnID{0}, PredicateCondition::Equals, 1234.1f, {}},
      {ColumnID{0}, PredicateCondition::NotEquals, 123.0f, {12345, 1234}},
      {ColumnID{0}, PredicateCondition::NotEquals, 123.1f, {12345, 123, 1234}},
      {ColumnID{0}, PredicateCondition::GreaterThan, 1234.1f, {12345}},
      {ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234.1f, {12345}},
      {ColumnID{0}, PredicateCondition::LessThan, 1234.0, {123}},
      {ColumnID{0}, PredicateCondition::LessThan, 1234.1, {123, 1234}},
      {ColumnID{0}, PredicateCondition::LessThanEquals, 1234.0, {123, 1234}},
      {ColumnID{0}, PredicateCondition::LessThanEquals, 1234.1, {123, 1234}}};

  const auto table = get_int_float_op();
  for (const auto& [column_id, predicate_condition, value, expected_values] : predicates) {
    const auto scan = create_table_scan(table, column_id, predicate_condition, value);
    scan->execute();
    ASSERT_COLUMN_EQ(scan->get_output(), column_id, expected_values);
  }
}

TEST_P(OperatorsTableScanTest, SetParameters) {
  const auto parameters = std::unordered_map<ParameterID, AllTypeVariant>{{ParameterID{3}, AllTypeVariant{5}},
                                                                          {ParameterID{2}, AllTypeVariant{6}}};

  const auto column = get_column_expression(_int_int_compressed, ColumnID{0});

  const auto scan_a = std::make_shared<TableScan>(_int_int_compressed, greater_than_equals_(column, 4));
  scan_a->set_parameters(parameters);
  EXPECT_EQ(*scan_a->predicate(), *greater_than_equals_(column, 4));

  const auto parameter_expression_with_value = placeholder_(ParameterID{2});
  const auto scan_b =
      std::make_shared<TableScan>(_int_int_compressed, greater_than_equals_(column, placeholder_(ParameterID{2})));
  scan_b->set_parameters(parameters);
  EXPECT_EQ(*scan_b->predicate(), *greater_than_equals_(column, parameter_expression_with_value));

  const auto scan_c =
      std::make_shared<TableScan>(_int_int_compressed, greater_than_equals_(column, placeholder_(ParameterID{4})));
  scan_c->set_parameters(parameters);
  EXPECT_EQ(*scan_c->predicate(), *greater_than_equals_(column, placeholder_(ParameterID{4})));
}

TEST_P(OperatorsTableScanTest, GetImpl) {
  /**
   * Test that the correct scanning backend is chosen
   */

  const auto column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "a");
  const auto column_b = pqp_column_(ColumnID{1}, DataType::Float, false, "b");
  const auto column_s = pqp_column_(ColumnID{1}, DataType::String, false, "c");
  const auto column_an = pqp_column_(ColumnID{0}, DataType::String, true, "a");

  // clang-format off
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(TableScan{get_int_float_op(), equals_(column_a, 5)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(TableScan{get_int_float_op(), equals_(5, column_a)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(TableScan{get_int_float_op(), equals_(5, column_a)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(TableScan{get_int_float_op(), equals_(5.0f, column_a)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(TableScan{get_int_float_op(), equals_(5, column_b)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(TableScan{get_int_float_op(), equals_(5.5, column_b)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnVsValueTableScanImpl*>(TableScan{get_int_float_op(), equals_(5.5f, column_b)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnBetweenTableScanImpl*>(TableScan{get_int_float_op(), between_inclusive_(column_a, 0, 5)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnBetweenTableScanImpl*>(TableScan{get_int_float_op(), between_inclusive_(column_a, 0.0f, 5.0)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnVsColumnTableScanImpl*>(TableScan{get_int_float_op(), equals_(column_b, column_a)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnLikeTableScanImpl*>(TableScan{get_int_string_op(), like_(column_s, "%s%")}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_string_op(), like_("hello", "%s%")}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), in_(column_a, list_(1, 2, 3))}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), in_(column_a, list_(1, 2, 3))}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), and_(greater_than_(column_a, 5), less_than_(column_b, 6))}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), greater_than_(column_a, 5.5f)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), greater_than_(column_b, 1e40)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), greater_than_(column_a, int64_t{3'000'000'000})}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), equals_(column_a, "hello")}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), between_inclusive_(column_a, 0.1f, 5.5)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ExpressionEvaluatorTableScanImpl*>(TableScan{get_int_float_op(), between_inclusive_(column_a, 0, null_())}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnIsNullTableScanImpl*>(TableScan{get_int_float_with_null_op(), is_null_(column_an)}.create_impl().get()));  // NOLINT
  EXPECT_TRUE(dynamic_cast<ColumnIsNullTableScanImpl*>(TableScan{get_int_float_with_null_op(), is_not_null_(column_an)}.create_impl().get()));  // NOLINT

  // Cases where the lossless_predicate_cast is used and the predicate condition gets adjusted:
  {
    const auto abstract_impl = TableScan{get_int_float_op(), greater_than_(column_b, 3.1)}.create_impl();
    const auto impl = dynamic_cast<ColumnVsValueTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::GreaterThanEquals);
    EXPECT_EQ(impl->value, AllTypeVariant{3.1000001430511474609375f});
  }
  {
    const auto abstract_impl = TableScan{get_int_float_op(), greater_than_equals_(column_b, 3.1)}.create_impl();
    const auto impl = dynamic_cast<ColumnVsValueTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::GreaterThanEquals);
    EXPECT_EQ(impl->value, AllTypeVariant{3.1000001430511474609375f});
  }
  {
    const auto abstract_impl = TableScan{get_int_float_op(), equals_(column_b, 3.1)}.create_impl();
    const auto impl = dynamic_cast<ExpressionEvaluatorTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    // Cannot losslessly convert here.
  }
  {
    const auto abstract_impl = TableScan{get_int_float_op(), less_than_(column_b, 3.1)}.create_impl();
    const auto impl = dynamic_cast<ColumnVsValueTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::LessThanEquals);
    EXPECT_EQ(impl->value, AllTypeVariant{3.099999904632568359375f});
  }
  {
    const auto abstract_impl = TableScan{get_int_float_op(), less_than_equals_(column_b, 3.1)}.create_impl();
    const auto impl = dynamic_cast<ColumnVsValueTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::LessThanEquals);
    EXPECT_EQ(impl->value, AllTypeVariant{3.099999904632568359375f});
  }

  // Test it once with reversed roles:
  {
    const auto abstract_impl = TableScan{get_int_float_op(), greater_than_(3.1, column_b)}.create_impl();
    const auto impl = dynamic_cast<ColumnVsValueTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::LessThanEquals);
    EXPECT_EQ(impl->value, AllTypeVariant{3.099999904632568359375f});
  }

  // The same for different types of between:
  {
    const auto abstract_impl = TableScan{get_int_float_op(), between_inclusive_(column_b, 3.1, 4.1)}.create_impl();
    const auto impl = dynamic_cast<ColumnBetweenTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::BetweenInclusive);
    EXPECT_EQ(impl->left_value, AllTypeVariant{3.1000001430511474609375f});
    EXPECT_EQ(impl->right_value, AllTypeVariant{4.099999904632568359375f});
  }

  {
    const auto abstract_impl = TableScan{get_int_float_op(), between_inclusive_(column_b, 3.1, 4.0)}.create_impl();
    const auto impl = dynamic_cast<ColumnBetweenTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::BetweenInclusive);
    EXPECT_EQ(impl->left_value, AllTypeVariant{3.1000001430511474609375f});
    EXPECT_EQ(impl->right_value, AllTypeVariant{4.0f});
  }

  {
    const auto abstract_impl = TableScan{get_int_float_op(), between_upper_exclusive_(column_b, 3.1, 4.0)}.create_impl();  // NOLINT
    const auto impl = dynamic_cast<ColumnBetweenTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::BetweenUpperExclusive);
    EXPECT_EQ(impl->left_value, AllTypeVariant{3.1000001430511474609375f});
    EXPECT_EQ(impl->right_value, AllTypeVariant{4.0f});
  }

  {
    const auto abstract_impl = TableScan{get_int_float_op(), between_exclusive_(column_b, 3.1, 4.0)}.create_impl();
    const auto impl = dynamic_cast<ColumnBetweenTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::BetweenUpperExclusive);
    EXPECT_EQ(impl->left_value, AllTypeVariant{3.1000001430511474609375f});
    EXPECT_EQ(impl->right_value, AllTypeVariant{4.0f});
  }

  {
    const auto abstract_impl = TableScan{get_int_float_op(), between_exclusive_(column_b, 3.1, 4.1)}.create_impl();
    const auto impl = dynamic_cast<ColumnBetweenTableScanImpl*>(abstract_impl.get());
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->predicate_condition, PredicateCondition::BetweenInclusive);
    EXPECT_EQ(impl->left_value, AllTypeVariant{3.1000001430511474609375f});
    EXPECT_EQ(impl->right_value, AllTypeVariant{4.099999904632568359375f});
  }

  // clang-format on
}

TEST_P(OperatorsTableScanTest, TwoBigScans) {
  // To stress-test the SIMD scan, which only operates on bigger tables, the generated table holds 1'000 rows.
  // For each fifth row, column a is NULL. Otherwise, a is 100'000 + i, b is the index in the list of non-NULL values.

  auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, true}, {"b", DataType::Int, true}};
  const auto data_table = std::make_shared<Table>(column_definitions, TableType::Data, 13);

  for (auto i = 0, index = 0; i < 1'000; ++i) {
    if (i % 5 == 4) {
      data_table->append({NullValue{}, i});
    } else {
      data_table->append({100'000 + i, index++});
    }
  }

  // We have two full chunks and one open chunk, we only encode the full chunks
  for (auto chunk_id = ChunkID{0}; chunk_id < 2; ++chunk_id) {
    ChunkEncoder::encode_chunk(data_table->get_chunk(chunk_id), {DataType::Int, DataType::Int},
                               {_encoding_type, EncodingType::Unencoded});
  }

  auto data_table_wrapper = std::make_shared<TableWrapper>(data_table);
  data_table_wrapper->execute();

  const auto column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "a");

  // Scan for a >= 100'100
  const auto scan_a = std::make_shared<TableScan>(data_table_wrapper, greater_than_equals_(column_a, 100'100));
  scan_a->execute();

  // Now scan for b <= 100'700
  const auto scan_b = std::make_shared<TableScan>(scan_a, less_than_equals_(column_a, 100'700));
  scan_b->execute();

  // Try the same with a between scan
  const auto between_scan =
      std::make_shared<TableScan>(data_table_wrapper, between_inclusive_(column_a, 100'100, 100'700));
  between_scan->execute();

  EXPECT_TABLE_EQ_UNORDERED(scan_b->get_output(), between_scan->get_output());

  const auto& table = scan_b->get_output();
  // Out of the 600 values, 120 are NULL and should have been skipped.
  EXPECT_EQ(table->row_count(), 481);
  for (auto i = 0; i < 481; ++i) {
    EXPECT_EQ(table->get_value<int32_t>(ColumnID{1}, i), 80 + i);
  }
}

}  // namespace opossum
