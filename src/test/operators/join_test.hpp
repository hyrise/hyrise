#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_join_operator.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This is the basic and typed JoinTest class.
It contains all tables that are currently used for join tests.
The actual test cases are split into EquiOnly and FullJoin tests.
*/

class JoinTest : public BaseTest {
 public:
  static void SetUpTestCase() {  // called ONCE before tests are run
    // load and create regular ValueSegment tables
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float2.tbl", 2));
    _table_wrapper_c = std::make_shared<TableWrapper>(load_table("src/test/tables/int_string.tbl", 4));
    _table_wrapper_d = std::make_shared<TableWrapper>(load_table("src/test/tables/string_int.tbl", 3));
    _table_wrapper_e = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int.tbl", 4));
    _table_wrapper_f = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int2.tbl", 4));
    _table_wrapper_g = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int3.tbl", 4));
    _table_wrapper_h = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int4.tbl", 4));
    _table_wrapper_i = std::make_shared<TableWrapper>(load_table("src/test/tables/int5.tbl", 1));
    _table_wrapper_j = std::make_shared<TableWrapper>(load_table("src/test/tables/int3.tbl", 1));
    _table_wrapper_k = std::make_shared<TableWrapper>(load_table("src/test/tables/int4.tbl", 1));
    _table_wrapper_l = std::make_shared<TableWrapper>(load_table("src/test/tables/int.tbl", 1));
    _table_wrapper_m = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 20));
    _table_wrapper_n = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 20));
    _table_wrapper_o = std::make_shared<TableWrapper>(load_table("src/test/tables/float_zero_precision.tbl", 1));
    _table_wrapper_p = std::make_shared<TableWrapper>(load_table("src/test/tables/double_zero_precision.tbl", 1));
    _table_wrapper_q = std::make_shared<TableWrapper>(load_table("src/test/tables/string_numbers.tbl", 1));

    // load and create DictionarySegment tables
    auto table = load_table("src/test/tables/int_float.tbl", 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}, ChunkID{1}});
    _table_wrapper_a_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/int_float2.tbl", 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}, ChunkID{1}});
    _table_wrapper_b_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/int_float.tbl", 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}});
    _table_wrapper_c_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 20);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}});
    _table_wrapper_m_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 20);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}});
    _table_wrapper_n_dict = std::make_shared<TableWrapper>(std::move(table));

    // execute all TableWrapper operators in advance
    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_wrapper_c->execute();
    _table_wrapper_d->execute();
    _table_wrapper_e->execute();
    _table_wrapper_f->execute();
    _table_wrapper_g->execute();
    _table_wrapper_h->execute();
    _table_wrapper_i->execute();
    _table_wrapper_j->execute();
    _table_wrapper_k->execute();
    _table_wrapper_l->execute();
    _table_wrapper_m->execute();
    _table_wrapper_n->execute();
    _table_wrapper_o->execute();
    _table_wrapper_p->execute();
    _table_wrapper_q->execute();
    _table_wrapper_a_dict->execute();
    _table_wrapper_b_dict->execute();
    _table_wrapper_c_dict->execute();
    _table_wrapper_m_dict->execute();
    _table_wrapper_n_dict->execute();
  }

 protected:
  void SetUp() override {}

  // builds and executes the given Join and checks correctness of the output
  template <typename JoinType>
  void test_join_output(const std::shared_ptr<const AbstractOperator>& left,
                        const std::shared_ptr<const AbstractOperator>& right, const ColumnIDPair& column_ids,
                        const PredicateCondition predicate_condition, const JoinMode mode, const std::string& file_name,
                        size_t chunk_size) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // build and execute join
    auto join = std::make_shared<JoinType>(left, right, mode, column_ids, predicate_condition);
    EXPECT_NE(join, nullptr) << "Could not build Join";
    join->execute();

    EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);
  }

  inline static std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d,
      _table_wrapper_e, _table_wrapper_f, _table_wrapper_g, _table_wrapper_h, _table_wrapper_i, _table_wrapper_j,
      _table_wrapper_k, _table_wrapper_l, _table_wrapper_m, _table_wrapper_n, _table_wrapper_o, _table_wrapper_p,
      _table_wrapper_q, _table_wrapper_a_dict, _table_wrapper_b_dict, _table_wrapper_c_dict, _table_wrapper_m_dict,
      _table_wrapper_n_dict;
};
}  // namespace opossum
