#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_join_operator.hpp"
#include "operators/join_hash.hpp"
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
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float2.tbl", 2));
    _table_wrapper_c = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_string.tbl", 4));
    _table_wrapper_d = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/string_int.tbl", 3));
    _table_wrapper_e = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int.tbl", 4));
    _table_wrapper_f = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int2.tbl", 4));
    _table_wrapper_g = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int3.tbl", 4));
    _table_wrapper_h = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int4.tbl", 4));
    _table_wrapper_i = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int5.tbl", 1));
    _table_wrapper_j = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int3.tbl", 1));
    _table_wrapper_k = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int4.tbl", 1));
    _table_wrapper_l = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int.tbl", 1));
    _table_wrapper_m = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 20));
    _table_wrapper_n = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 20));
    _table_wrapper_o =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/float_zero_precision.tbl", 1));
    _table_wrapper_p =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/double_zero_precision.tbl", 1));
    _table_wrapper_q = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/string_numbers.tbl", 1));

    // load and create DictionarySegment tables
    auto table = load_table("resources/test_data/tbl/int_float.tbl", 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}, ChunkID{1}});
    _table_wrapper_a_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("resources/test_data/tbl/int_float2.tbl", 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}, ChunkID{1}});
    _table_wrapper_b_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("resources/test_data/tbl/int_float.tbl", 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}});
    _table_wrapper_c_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 20);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}});
    _table_wrapper_m_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 20);
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
                        size_t chunk_size, std::vector<OperatorJoinPredicate> additional_join_predicates = {}) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // build and execute join
    std::shared_ptr<AbstractJoinOperator> join;

    if (std::is_same<JoinType, JoinHash>::value) {
      join = std::make_shared<JoinHash>(left, right, mode, column_ids, predicate_condition, std::nullopt,
                                        additional_join_predicates);
    } else {
      join = std::make_shared<JoinType>(left, right, mode, column_ids, predicate_condition);
    }

    EXPECT_NE(join, nullptr) << "Could not build Join";

    join->execute();

    const auto actual_result = join->get_output();
    EXPECT_TABLE_EQ_UNORDERED(actual_result, expected_result);

    // Test the column definitions of the output table, especially the nullability
    for (auto output_column_id = ColumnID{0}; output_column_id < actual_result->column_count(); ++output_column_id) {
      auto expected_column_definition = TableColumnDefinition{};

      switch (mode) {
        case JoinMode::Inner:
        case JoinMode::Left:
        case JoinMode::Right:
        case JoinMode::FullOuter:
        case JoinMode::Cross:
          if (output_column_id < left->get_output()->column_count()) {
            expected_column_definition = left->get_output()->column_definitions()[output_column_id];
            if (mode == JoinMode::Right || mode == JoinMode::FullOuter) expected_column_definition.nullable = true;
          } else {
            expected_column_definition =
                right->get_output()->column_definitions()[output_column_id - left->get_output()->column_count()];
            if (mode == JoinMode::Left || mode == JoinMode::FullOuter) expected_column_definition.nullable = true;
          }
          break;

        case JoinMode::Semi:
        case JoinMode::Anti:
          expected_column_definition = left->get_output()->column_definitions()[output_column_id];
          break;
      }
      const auto actual_column_definition = actual_result->column_definitions()[output_column_id];
      EXPECT_EQ(actual_column_definition, expected_column_definition);
    }
  }

  inline static std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d,
      _table_wrapper_e, _table_wrapper_f, _table_wrapper_g, _table_wrapper_h, _table_wrapper_i, _table_wrapper_j,
      _table_wrapper_k, _table_wrapper_l, _table_wrapper_m, _table_wrapper_n, _table_wrapper_o, _table_wrapper_p,
      _table_wrapper_q, _table_wrapper_a_dict, _table_wrapper_b_dict, _table_wrapper_c_dict, _table_wrapper_m_dict,
      _table_wrapper_n_dict;
};
}  // namespace opossum
