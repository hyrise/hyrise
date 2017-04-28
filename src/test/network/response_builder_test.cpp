#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include "../../lib/network/generated/opossum.grpc.pb.h"
#pragma GCC diagnostic pop
#include "../../lib/network/response_builder.hpp"
#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/chunk_compression.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace {
std::string load_response(const std::string &file_name) {
  std::ifstream infile(file_name);
  std::string line;
  std::stringstream response_text;

  while (std::getline(infile, line)) {
    response_text << line << std::endl;
  }

  return response_text.str();
}
}  // namespace

namespace opossum {

class ResponseBuilderTest : public BaseTest {
 protected:
  void SetUp() override {
    auto test_table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt = std::make_shared<GetTable>("table_a");
    _gt->execute();

    auto test_table_dict = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_dict", test_table_dict);

    auto compression = std::make_unique<ChunkCompression>("table_dict", std::vector<ChunkID>{0u, 1u}, false);
    compression->execute();

    _gt_dict = std::make_shared<GetTable>("table_dict");
    _gt_dict->execute();
  }

  std::shared_ptr<GetTable> _gt, _gt_dict;
  ResponseBuilder _builder;
};

TEST_F(ResponseBuilderTest, BuildResponseValueColumn) {
  proto::Response response;
  auto expected_result = load_response("src/test/responses/int_float.tbl.rsp");

  _builder.build_response(response, _gt->get_output());

  EXPECT_EQ(response.DebugString(), expected_result);
}

TEST_F(ResponseBuilderTest, BuildResponseDictColumn) {
  proto::Response response;
  auto expected_result = load_response("src/test/responses/int_float.tbl.rsp");

  _builder.build_response(response, _gt_dict->get_output());

  EXPECT_EQ(response.DebugString(), expected_result);
}

TEST_F(ResponseBuilderTest, BuildResponseRefColumn) {
  proto::Response response;
  auto expected_result = load_response("src/test/responses/int_float_filtered_a_1234.tbl.rsp");

  auto scan_1 = std::make_shared<TableScan>(_gt, "a", "=", 1234);
  scan_1->execute();
  _builder.build_response(response, scan_1->get_output());

  EXPECT_EQ(response.DebugString(), expected_result);
}

}  // namespace opossum
