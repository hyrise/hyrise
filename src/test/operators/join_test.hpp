#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_join_operator.hpp"
#include "../../lib/operators/chunk_compression.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

/*
This is the basic and typed JoinTest class.
It contains all tables that are currently used for join tests.
The actual testcases are split into EquiOnly and FullJoin tests.
*/

class JoinTest : public BaseTest {
 protected:
  void SetUp() override {
    // load and create regular ValueColumn tables
    std::shared_ptr<Table> table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table));
    _gt_a = std::make_shared<GetTable>("table_a");

    table = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table));
    _gt_b = std::make_shared<GetTable>("table_b");

    table = load_table("src/test/tables/int_string.tbl", 4);
    StorageManager::get().add_table("table_c", std::move(table));
    _gt_c = std::make_shared<GetTable>("table_c");

    table = load_table("src/test/tables/string_int.tbl", 3);
    StorageManager::get().add_table("table_d", std::move(table));
    _gt_d = std::make_shared<GetTable>("table_d");

    table = load_table("src/test/tables/int_int.tbl", 4);
    StorageManager::get().add_table("table_e", std::move(table));
    _gt_e = std::make_shared<GetTable>("table_e");

    table = load_table("src/test/tables/int_int2.tbl", 4);
    StorageManager::get().add_table("table_f", std::move(table));
    _gt_f = std::make_shared<GetTable>("table_f");

    table = load_table("src/test/tables/int_int3.tbl", 4);
    StorageManager::get().add_table("table_g", std::move(table));
    _gt_g = std::make_shared<GetTable>("table_g");

    table = load_table("src/test/tables/int_int4.tbl", 4);
    StorageManager::get().add_table("table_h", std::move(table));
    _gt_h = std::make_shared<GetTable>("table_h");

    // load and create DictionaryColumn tables
    table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a_dict", table);
    _gt_a_dict = std::make_shared<GetTable>("table_a_dict");
    {
      auto compression = std::make_unique<ChunkCompression>("table_a_dict", std::vector<ChunkID>{0u, 1u}, false);
      compression->execute();
    }

    table = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b_dict", table);
    _gt_b_dict = std::make_shared<GetTable>("table_b_dict");
    {
      auto compression = std::make_unique<ChunkCompression>("table_b_dict", std::vector<ChunkID>{0u, 1u}, false);
      compression->execute();
    }

    table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_c_dict", table);
    _gt_c_dict = std::make_shared<GetTable>("table_c_dict");
    {
      auto compression = std::make_unique<ChunkCompression>("table_c_dict", 0u, false);
      compression->execute();
    }

    // execute all GetTable operators in advance
    _gt_a->execute();
    _gt_b->execute();
    _gt_c->execute();
    _gt_d->execute();
    _gt_e->execute();
    _gt_f->execute();
    _gt_g->execute();
    _gt_h->execute();
    _gt_a_dict->execute();
    _gt_b_dict->execute();
    _gt_c_dict->execute();
  }

  // builds and executes the given Join and checks correctness of the output
  template <typename JoinType>
  void test_join_output(const std::shared_ptr<const AbstractOperator> left,
                        const std::shared_ptr<const AbstractOperator> right,
                        const std::pair<std::string, std::string> &column_names, const std::string &op,
                        const JoinMode mode, const std::string &prefix_left, const std::string &prefix_right,
                        const std::string &file_name, size_t chunk_size) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // build and execute join
    auto join = std::make_shared<JoinType>(left, right, column_names, op, mode, prefix_left, prefix_right);
    EXPECT_NE(join, nullptr) << "Could not build Join";
    join->execute();

    EXPECT_TABLE_EQ(join->get_output(), expected_result);
  }

  std::shared_ptr<GetTable> _gt_a, _gt_b, _gt_c, _gt_d, _gt_e, _gt_f, _gt_g, _gt_h, _gt_a_dict, _gt_b_dict, _gt_c_dict;
};

}  // namespace opossum
