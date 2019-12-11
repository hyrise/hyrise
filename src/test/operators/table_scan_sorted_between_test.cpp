#include <memory>

#include "typed_ordered_operator_base_test.hpp"
#include "gtest/gtest.h"
#include "operators/table_wrapper.hpp"
#include "operators/table_scan/sorted_segment_between_search.hpp"


namespace opossum {

class TableScanSortedBetweenTest : public TypedOrderedOperatorBaseTest {
  protected:
    std::shared_ptr<AbstractOperator> _data_table_wrapper;


    void SetUp() override {
      const auto& [data_type, encoding, ordered_by_mode, nullable] = GetParam();

      auto column_definitions = TableColumnDefinitions{{"a", data_type, nullable}, {"b", DataType::Int, nullable}};

      const auto data_table = std::make_shared<Table>(column_definitions, TableType::Data, 6);

      // `nullable=nullable` is a dirty hack to work around C++ defect 2313.
      resolve_data_type(data_type, [&, nullable = nullable](const auto type) {
        using Type = typename decltype(type)::type;
        for (auto i = 0; i <= 10; ++i) {
          auto double_value = 10.25 + i * 2.0;

          if (nullable && i % 3 == 2) {
            data_table->append({NullValue{}, i});
          } else {
            if constexpr (std::is_same_v<pmr_string, Type>) {
              data_table->append({pmr_string{std::to_string(double_value)}, i});
            } else {
              data_table->append({static_cast<Type>(double_value), i});
            }
          }
        }
      });

      data_table->last_chunk()->finalize();

      // We have two full chunks and one open chunk, we only encode the full chunks
      for (auto chunk_id = ChunkID{0}; chunk_id < 2; ++chunk_id) {
        ChunkEncoder::encode_chunk(data_table->get_chunk(chunk_id), {data_type, DataType::Int},
                                   {encoding, EncodingType::Unencoded});
      }


      // TODO Sort

      _data_table_wrapper = std::make_shared<TableWrapper>(data_table);
      _data_table_wrapper->execute();
    }

};


INSTANTIATE_TEST_SUITE_P(TableScanSortedBetweenTestInstances, TableScanSortedBetweenTest, testing::ValuesIn(create_test_params()),
                         TypedOrderedOperatorBaseTest::format);

} // namespace opossum
