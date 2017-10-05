#include <fstream>
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
#include "network/generated/opossum.grpc.pb.h"
#pragma GCC diagnostic pop
#include "network/response_builder.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace {
std::string load_response(const std::string &file_name) {
  std::string line;
  std::stringstream response_text;

  std::ifstream infile(file_name);
  opossum::Assert(infile.is_open(), "load_response: Could not find file " + file_name);

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
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper->execute();

    auto test_table_dict = load_table("src/test/tables/int_float.tbl", 2);

    DictionaryCompression::compress_table(*test_table_dict);

    _table_wrapper_dict = std::make_shared<TableWrapper>(std::move(test_table_dict));
    _table_wrapper_dict->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_dict;
  ResponseBuilder _builder;
};

TEST_F(ResponseBuilderTest, BuildResponseValueColumn) {
  proto::Response response;
  auto expected_result = load_response("src/test/responses/int_float.tbl.rsp");

  _builder.build_response(response, _table_wrapper->get_output());

  EXPECT_EQ(response.DebugString(), expected_result);
}

TEST_F(ResponseBuilderTest, BuildResponseDictColumn) {
  proto::Response response;
  auto expected_result = load_response("src/test/responses/int_float.tbl.rsp");

  _builder.build_response(response, _table_wrapper_dict->get_output());

  EXPECT_EQ(response.DebugString(), expected_result);
}

TEST_F(ResponseBuilderTest, BuildResponseRefColumn) {
  proto::Response response;
  auto expected_result = load_response("src/test/responses/int_float_filtered_a_1234.tbl.rsp");

  auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpEquals, 1234);
  scan_1->execute();
  _builder.build_response(response, scan_1->get_output());

  EXPECT_EQ(response.DebugString(), expected_result);
}

}  // namespace opossum
