#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/materialize.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorsMaterializeTest : public BaseTest {};

TEST_F(OperatorsMaterializeTest, MaterializeUncompressedTable) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 3);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto materialize = std::make_shared<Materialize>(table_wrapper);
  materialize->execute();

  EXPECT_TABLE_EQ_ORDERED(materialize->get_output(), table);
}

TEST_F(OperatorsMaterializeTest, MaterializeCompressedTable) {
  auto table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 3);
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto materialize = std::make_shared<Materialize>(table_wrapper);
  materialize->execute();

  EXPECT_TABLE_EQ_ORDERED(materialize->get_output(), table);
}

}  // namespace opossum
