#include "gtest/gtest.h"

#include "storage/tuple/tuple_sequence.hpp"
#include "storage/tuple/tuple_format.hpp"
#include "storage/tuple/tuple_sequencer.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

class TupleSequenceTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto column_definitions = TableColumnDefinitions{
        {"a", DataType::Int, false},
        {"b", DataType::Int, true},
        {"c", DataType::Double, false},
        {"d", DataType::Double, true},
        {"e", DataType::String, false},
        {"f", DataType::String, true}
    };

    table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

    // clang-format off
    table->append({1,  2,           3.5,  4.5,         "five",       "six"});
    table->append({7,  NullValue{}, 8.5,  NullValue{}, "nine",       NullValue{}});
    table->append({11, 12,          13.5, NullValue{}, "fourteen",   NullValue{}});
    table->append({15, NullValue{}, 16.5, 17.5,        "eighteen",   "eighteen2"});
    table->append({20, 21,          22.5, 23.5,        "twentyfour", NullValue{}});
    // clang-format on
  }

  std::shared_ptr<Table> table;
};

TEST_F(TupleSequenceTest, InterleaveAndDeinterleave) {

}




}  // namespace
