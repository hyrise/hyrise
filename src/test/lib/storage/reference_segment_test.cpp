#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

class ReferenceSegmentTest : public BaseTest {
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, true);
    column_definitions.emplace_back("b", DataType::Float, false);

    _test_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3});
    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});
    _test_table->append({12345, 458.7f});
    _test_table->append({NULL_VALUE, 458.7f});
    _test_table->append({12345, 458.7f});

    TableColumnDefinitions column_definitions2;
    column_definitions2.emplace_back("a", DataType::Int, false);
    column_definitions2.emplace_back("b", DataType::Int, false);
    _test_table_dict = std::make_shared<Table>(column_definitions2, TableType::Data, ChunkOffset{5}, UseMvcc::Yes);
    for (auto index = int32_t{0}; index <= 24; index += 2) {
      _test_table_dict->append({index, 100 + index});
    }

    ChunkEncoder::encode_chunks(_test_table_dict, {ChunkID{0}, ChunkID{1}});

    Hyrise::get().storage_manager.add_table("test_table_dict", _test_table_dict);
  }

 public:
  std::shared_ptr<Table> _test_table, _test_table_dict;
};

TEST_F(ReferenceSegmentTest, RetrievesValues) {
  // RowIDPosList with (0, 0), (0, 1), (0, 2)
  auto pos_list = std::make_shared<RowIDPosList>(std::initializer_list<RowID>(
      {RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{2}}}));
  auto ref_segment = ReferenceSegment(_test_table, ColumnID{0}, pos_list);

  auto& segment = *(_test_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));

  EXPECT_EQ(ref_segment[ChunkOffset{0}], segment[ChunkOffset{0}]);
  EXPECT_EQ(ref_segment[ChunkOffset{1}], segment[ChunkOffset{1}]);
  EXPECT_EQ(ref_segment[ChunkOffset{2}], segment[ChunkOffset{2}]);
}

TEST_F(ReferenceSegmentTest, RetrievesValuesOutOfOrder) {
  // RowIDPosList with (0, 1), (0, 2), (0, 0)
  auto pos_list = std::make_shared<RowIDPosList>(std::initializer_list<RowID>(
      {RowID{ChunkID{0}, ChunkOffset{1}}, RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{0}, ChunkOffset{0}}}));
  auto ref_segment = ReferenceSegment(_test_table, ColumnID{0}, pos_list);

  auto& segment = *(_test_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));

  EXPECT_EQ(ref_segment[ChunkOffset{0}], segment[ChunkOffset{1}]);
  EXPECT_EQ(ref_segment[ChunkOffset{1}], segment[ChunkOffset{2}]);
  EXPECT_EQ(ref_segment[ChunkOffset{2}], segment[ChunkOffset{0}]);
}

TEST_F(ReferenceSegmentTest, RetrievesValuesFromChunks) {
  // RowIDPosList with (0, 2), (1, 0), (1, 1)
  auto pos_list = std::make_shared<RowIDPosList>(std::initializer_list<RowID>(
      {RowID{ChunkID{0}, ChunkOffset{2}}, RowID{ChunkID{1}, ChunkOffset{0}}, RowID{ChunkID{1}, ChunkOffset{1}}}));
  auto ref_segment = ReferenceSegment(_test_table, ColumnID{0}, pos_list);

  auto& segment_1 = *(_test_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));
  auto& segment_2 = *(_test_table->get_chunk(ChunkID{1})->get_segment(ColumnID{0}));

  EXPECT_EQ(ref_segment[ChunkOffset{0}], segment_1[ChunkOffset{2}]);
  EXPECT_TRUE(variant_is_null(ref_segment[ChunkOffset{1}]) && variant_is_null(segment_2[ChunkOffset{0}]));
  EXPECT_EQ(ref_segment[ChunkOffset{2}], segment_2[ChunkOffset{1}]);
}

TEST_F(ReferenceSegmentTest, RetrieveNullValueFromNullRowID) {
  // RowIDPosList with (0, 0), (0, 1), NULL_ROW_ID, (0, 2)
  auto pos_list = std::make_shared<RowIDPosList>(
      std::initializer_list<RowID>({RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}}, NULL_ROW_ID,
                                    RowID{ChunkID{0}, ChunkOffset{2}}}));

  auto ref_segment = ReferenceSegment(_test_table, ColumnID{0}, pos_list);

  auto& segment = *(_test_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0u}));

  EXPECT_EQ(ref_segment[ChunkOffset{0}], segment[ChunkOffset{0}]);
  EXPECT_EQ(ref_segment[ChunkOffset{1}], segment[ChunkOffset{1}]);
  EXPECT_TRUE(variant_is_null(ref_segment[ChunkOffset{2}]));
  EXPECT_EQ(ref_segment[ChunkOffset{3}], segment[ChunkOffset{2}]);
}

TEST_F(ReferenceSegmentTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */

  const auto pos_list_a = std::make_shared<RowIDPosList>();
  pos_list_a->emplace_back(RowID{ChunkID{0}, ChunkOffset{0}});
  pos_list_a->emplace_back(RowID{ChunkID{0}, ChunkOffset{1}});
  const auto pos_list_b = std::make_shared<RowIDPosList>();

  ReferenceSegment reference_segment_a(_test_table, ColumnID{0}, pos_list_a);
  ReferenceSegment reference_segment_b(_test_table, ColumnID{0}, pos_list_b);

  EXPECT_EQ(reference_segment_a.memory_usage(MemoryUsageCalculationMode::Sampled),
            reference_segment_b.memory_usage(MemoryUsageCalculationMode::Sampled) + 2 * sizeof(RowID));
}

}  // namespace hyrise
