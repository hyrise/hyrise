#include "base_test.hpp"

#include <fstream>

#include "json.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_reference_operator.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "utils/make_bimap.hpp"

using namespace std::string_literals;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

enum class InputSide { Left, Right };

enum class InputTableType {
  // Input Tables are unencoded data
  Data,
  // Input Tables are reference Tables with all Segments of a Chunk having the same PosList
  SharedPosList,
  // Input Tables are reference Tables with each Segment using a different PosList
  IndividualPosLists
};

class BaseJoinOperatorFactory {
 public:
  virtual ~BaseJoinOperatorFactory() = default;
  virtual std::shared_ptr<AbstractJoinOperator> create_operator(
      const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
      const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
      const std::vector<OperatorJoinPredicate>& secondary_predicates = {}) = 0;
};

template <typename JoinOperator>
class JoinOperatorFactory : public BaseJoinOperatorFactory {
 public:
  std::shared_ptr<AbstractJoinOperator> create_operator(
      const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
      const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
      const std::vector<OperatorJoinPredicate>& secondary_predicates) override {
    return std::make_shared<JoinOperator>(left, right, mode, primary_predicate, secondary_predicates);
  }
};

const boost::bimap<InputTableType, std::string> input_table_type_to_string = make_bimap<InputTableType, std::string>({
    {InputTableType::Data, "No"},
    {InputTableType::SharedPosList, "Yes"},
    {InputTableType::IndividualPosLists, "Join"},
});

struct InputTableKey {
  InputSide side{};
  ChunkOffset chunk_size{};
  size_t table_size{};
  InputTableType input_table_type{};

  auto to_tuple() const { return std::tie(side, chunk_size, table_size, input_table_type); }
};

bool operator<(const InputTableKey& l, const InputTableKey& r) { return l.to_tuple() < r.to_tuple(); }

struct JoinTestConfiguration {
  InputTableKey input_left;
  InputTableKey input_right;
  JoinMode join_mode{JoinMode::Inner};
  DataType data_type_left{DataType::Int};
  DataType data_type_right{DataType::Int};
  bool nullable_left{false};
  bool nullable_right{false};
  PredicateCondition predicate_condition{PredicateCondition::Equals};
  std::vector<OperatorJoinPredicate> secondary_predicates;
  std::shared_ptr<BaseJoinOperatorFactory> join_operator_factory;

  void swap_input_sides() {
    std::swap(input_left, input_right);
    std::swap(data_type_left, data_type_right);
    std::swap(nullable_left, nullable_right);

    for (auto& secondary_predicate : secondary_predicates) {
      std::swap(secondary_predicate.column_ids.first, secondary_predicate.column_ids.second);
      secondary_predicate.predicate_condition = flip_predicate_condition(secondary_predicate.predicate_condition);
    }
  }
};

const std::unordered_map<DataType, size_t> data_type_order = {
    {DataType::Int, 0u}, {DataType::Float, 1u}, {DataType::Double, 2u}, {DataType::Long, 3u}, {DataType::String, 4u},
};

}  // namespace

namespace opossum {

class JoinTestRunner : public BaseTestWithParam<JoinTestConfiguration> {
 public:
  template <typename JoinOperator>
  static std::vector<JoinTestConfiguration> create_configurations() {
    auto configurations = std::vector<JoinTestConfiguration>{};

    /**
     * For each `all_*` set, the first element is the default - which is chosen to be sensible. Thus the elements are
     * not necessarily sorted.
     */
    auto all_data_types = std::vector<DataType>{};
    hana::for_each(data_type_pairs, [&](auto pair) {
      const DataType d = hana::first(pair);
      all_data_types.emplace_back(d);
    });

    const auto all_predicate_conditions = std::vector{
        PredicateCondition::Equals,      PredicateCondition::NotEquals,
        PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals,
        PredicateCondition::LessThan,    PredicateCondition::LessThanEquals,
    };
    const auto all_join_modes = std::vector{JoinMode::Inner,         JoinMode::Left, JoinMode::Right,
                                            JoinMode::FullOuter,     JoinMode::Semi, JoinMode::AntiNullAsFalse,
                                            JoinMode::AntiNullAsTrue};
    const auto all_left_table_sizes = std::vector{10u, 15u, 0u};
    const auto all_right_table_sizes = std::vector{10u, 15u, 0u};
    const auto all_left_nulls = std::vector{true, false};
    const auto all_right_nulls = std::vector{true, false};
    const auto all_chunk_sizes = std::vector{10u, 3u, 1u};
    const auto all_secondary_predicate_sets = std::vector<std::vector<OperatorJoinPredicate>>{
        {},
        {{{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan}},
        {{{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}},
        {{{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals}}
    };

    const auto all_swap_input_sides = std::vector{false, true};
    const auto all_input_table_types =
        std::vector{InputTableType::Data, InputTableType::IndividualPosLists, InputTableType::SharedPosList};

    // clang-format off
    JoinTestConfiguration default_configuration{
      InputTableKey{InputSide::Left, all_chunk_sizes.front(), all_left_table_sizes.front(), all_input_table_types.front()},
      InputTableKey{InputSide::Right, all_chunk_sizes.front(), all_right_table_sizes.front(), all_input_table_types.front()},
      JoinMode::Inner,
      DataType::Int,
      DataType::Int,
      false,
      false,
      PredicateCondition::Equals,
      all_secondary_predicate_sets.front(),
      std::make_shared<JoinOperatorFactory<JoinOperator>>()
    };
    // clang-format on

    const auto add_configuration_if_supported = [&](const auto& configuration) {
      // String vs non-String comparisons are not supported in Hyrise and therefore cannot be tested
      if ((configuration.data_type_left == DataType::String) != (configuration.data_type_right == DataType::String)) {
        return;
      }

      if (JoinOperator::supports(configuration.join_mode, configuration.predicate_condition,
                                 configuration.data_type_left, configuration.data_type_right, !configuration.secondary_predicates.empty())) {
        configurations.emplace_back(configuration);
      }
    };

//    for (const auto& data_type_left : all_data_types) {
//      for (const auto &data_type_right : all_data_types) {
//        for (const auto &predicate_condition : all_predicate_conditions) {
//          for (const auto left_table_size : all_left_table_sizes) {
//            for (const auto right_table_size : all_right_table_sizes) {
//              for (const auto &chunk_size : all_chunk_sizes) {
//                for (const auto &join_mode : all_join_modes) {
//                  for (const auto left_null : all_left_nulls) {
//                    for (const auto right_null : all_right_nulls) {
//                      for (const auto swap_input_sides : all_swap_input_sides) {
//                        for (const auto &secondary_predicates : all_secondary_predicate_sets) {
//                          auto join_test_configuration = default_configuration;
//                          join_test_configuration.data_type_left = data_type_left;
//                          join_test_configuration.data_type_right = data_type_right;
//                          join_test_configuration.predicate_condition = predicate_condition;
//                          join_test_configuration.input_left.table_size = left_table_size;
//                          join_test_configuration.input_right.table_size = right_table_size;
//                          join_test_configuration.input_left.chunk_size = chunk_size;
//                          join_test_configuration.input_right.chunk_size = chunk_size;
//                          join_test_configuration.join_mode = join_mode;
//                          join_test_configuration.nullable_left = left_null;
//                          join_test_configuration.nullable_right = right_null;
//                          join_test_configuration.secondary_predicates = secondary_predicates;
//
//                          if (swap_input_sides) {
//                            join_test_configuration.swap_input_sides();
//                          }
//
//                          add_configuration_if_supported(join_test_configuration);
//                        }
//                      }
//                    }
//                  }
//                }
//              }
//            }
//          }
//        }
//      }
//    }

    // JoinOperators (e.g. JoinHash) might pick a "common type"
    // Test that this works for all data_type_left/data_type_right combinations
    for (const auto data_type_left : all_data_types) {
      for (const auto data_type_right : all_data_types) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.data_type_left = data_type_left;
        join_test_configuration.data_type_right = data_type_right;

        add_configuration_if_supported(join_test_configuration);
      }
    }

    // JoinOperators (e.g. JoinHash) swap input sides depending on TableSize and JoinMode.
    // Test that predicate_condition and secondary predicates are flipped accordingly
    for (const auto predicate_condition : all_predicate_conditions) {
      for (const auto join_mode : all_join_modes) {
        for (const auto left_table_size : all_left_table_sizes) {
          for (const auto right_table_size : all_right_table_sizes) {
            for (const auto& secondary_predicates : all_secondary_predicate_sets) {
              auto join_test_configuration = default_configuration;
              join_test_configuration.predicate_condition = predicate_condition;
              join_test_configuration.join_mode = join_mode;
              join_test_configuration.input_left.table_size = left_table_size;
              join_test_configuration.input_right.table_size = right_table_size;
              join_test_configuration.secondary_predicates = secondary_predicates;

              add_configuration_if_supported(join_test_configuration);
            }
          }
        }
      }
    }

    // Anti* joins have different behaviours with NULL values.
    // Also test table sizes, as an empty right input table is a special case where a NULL value from the left side
    // would get emitted.
    for (const auto join_mode : {JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse}) {
      for (const auto left_table_size : all_left_table_sizes) {
        for (const auto right_table_size : all_right_table_sizes) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.join_mode = join_mode;
          join_test_configuration.nullable_left = true;
          join_test_configuration.nullable_right = true;
          join_test_configuration.input_left.table_size = left_table_size;
          join_test_configuration.input_right.table_size = right_table_size;

          add_configuration_if_supported(join_test_configuration);
        }
      }
    }

    // JoinOperators need to deal with differently sized Chunks (e.g., smaller last Chunk)
    // Trigger those via testing all table_size/chunk_size combinations
    for (const auto& left_table_size : all_left_table_sizes) {
      for (const auto& right_table_size : all_right_table_sizes) {
        for (const auto& chunk_size : all_chunk_sizes) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.input_left.table_size = left_table_size;
          join_test_configuration.input_right.table_size = right_table_size;
          join_test_configuration.input_left.chunk_size = chunk_size;
          join_test_configuration.input_right.chunk_size = chunk_size;

          add_configuration_if_supported(join_test_configuration);
        }
      }
    }

    // Different JoinModes have different handling of NULL values.
    // Additionally, JoinOperators (e.g., JoinSortMerge) have vastly different paths for different PredicateConditions
    // Test all combinations
    for (const auto& join_mode : all_join_modes) {
      for (const auto left_null : all_left_nulls) {
        for (const auto right_null : all_right_nulls) {
        for (const auto predicate_condition : all_predicate_conditions) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.join_mode = join_mode;
          join_test_configuration.nullable_left = left_null;
          join_test_configuration.nullable_right = right_null;
          join_test_configuration.predicate_condition = predicate_condition;

          add_configuration_if_supported(join_test_configuration);
        }
        }
      }
    }

    // The input tables are designed to have exclusive values. Test that these are handled correctly for different
    // JoinModes by swapping the input tables.
    // Additionally, go through all PredicateCondition to test especially the JoinSortMerge's different paths for these
    for (const auto& predicate_condition : all_predicate_conditions) {
      for (const auto& join_mode : all_join_modes) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.join_mode = join_mode;
        join_test_configuration.predicate_condition = predicate_condition;

        join_test_configuration.swap_input_sides();

        add_configuration_if_supported(join_test_configuration);
      }
    }

    // Test all combinations of reference/data input tables. This tests mostly the composition of the output table
    for (const auto& left_input_table_type : all_input_table_types) {
      for (const auto& right_input_table_type : all_input_table_types) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.input_left.input_table_type = left_input_table_type;
        join_test_configuration.input_right.input_table_type = right_input_table_type;

        add_configuration_if_supported(join_test_configuration);
      }
    }

    // Test MPJ support for all join modes. Swap the input tables to trigger cases especially in the Anti* modes
    // where a secondary predicate evaluating to FALSE might "save" a tuple from being discarded
    for (const auto& join_mode : all_join_modes) {
      for (const auto& secondary_predicates : all_secondary_predicate_sets) {
        for (const auto& predicate_condition : all_predicate_conditions) {
          for (const auto swap_input_sides : all_swap_input_sides) {
            auto join_test_configuration = default_configuration;
            join_test_configuration.join_mode = join_mode;
            join_test_configuration.secondary_predicates = secondary_predicates;
            join_test_configuration.predicate_condition = predicate_condition;

            if (swap_input_sides) {
              join_test_configuration.swap_input_sides();
            }

            add_configuration_if_supported(join_test_configuration);
          }
        }
      }
    }

    return configurations;
  }

  static std::string get_table_path(const InputTableKey& key) {
    const auto& [side, chunk_size, table_size, input_table_type] = key;

    const auto side_str = side == InputSide::Left ? "left" : "right";
    const auto table_size_str = std::to_string(table_size);

    return "resources/test_data/tbl/join_test_runner/input_table_"s + side_str + "_" + table_size_str + ".tbl";
  }

  static std::shared_ptr<Table> get_table(const InputTableKey& key) {
    auto input_table_iter = input_tables.find(key);
    if (input_table_iter == input_tables.end()) {
      const auto& [side, chunk_size, table_size, input_table_type] = key;
      std::ignore = side;
      std::ignore = table_size;

      auto table = load_table(get_table_path(key), chunk_size);

      /**
       * Create a Reference-Table pointing 1-to-1 and in-order to the rows in the original table
       */
      if (input_table_type != InputTableType::Data) {
        const auto reference_table = std::make_shared<Table>(table->column_definitions(), TableType::References);

        for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
          const auto input_chunk = table->get_chunk(chunk_id);

          Segments reference_segments;

          if (input_table_type == InputTableType::SharedPosList) {
            const auto pos_list = std::make_shared<PosList>();
            for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
              pos_list->emplace_back(chunk_id, chunk_offset);
            }

            for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
              reference_segments.emplace_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
            }

          } else if (input_table_type == InputTableType::IndividualPosLists) {
            for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
              const auto pos_list = std::make_shared<PosList>();
              for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
                pos_list->emplace_back(chunk_id, chunk_offset);
              }

              reference_segments.emplace_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
            }
          }

          reference_table->append_chunk(reference_segments);
        }

        table = reference_table;
      }

      input_table_iter = input_tables.emplace(key, table).first;
    }

    return input_table_iter->second;
  }

  static inline std::map<InputTableKey, std::shared_ptr<Table>> input_tables;
};

TEST_P(JoinTestRunner, TestJoin) {
  const auto configuration = GetParam();

  const auto input_table_left = get_table(configuration.input_left);
  const auto input_table_right = get_table(configuration.input_right);

  const auto input_op_left = std::make_shared<TableWrapper>(input_table_left);
  const auto input_op_right = std::make_shared<TableWrapper>(input_table_right);

  const auto column_id_left = ColumnID{static_cast<ColumnID::base_type>(
      2 * data_type_order.at(configuration.data_type_left) + (configuration.nullable_left ? 1 : 0))};
  const auto column_id_right = ColumnID{static_cast<ColumnID::base_type>(
      2 * data_type_order.at(configuration.data_type_right) + (configuration.nullable_right ? 1 : 0))};

  const auto primary_predicate =
      OperatorJoinPredicate{{column_id_left, column_id_right}, configuration.predicate_condition};

  const auto join_op = configuration.join_operator_factory->create_operator(
      input_op_left, input_op_right, configuration.join_mode, primary_predicate, configuration.secondary_predicates);
  const auto join_reference_op = std::make_shared<JoinReferenceOperator>(
      input_op_left, input_op_right, configuration.join_mode, primary_predicate, configuration.secondary_predicates);

  input_op_left->execute();
  input_op_right->execute();

  auto table_difference_message = std::optional<std::string>{};

  const auto print_configuration_info = [&]() {
    std::cout << "====================== JoinOperator ========================" << std::endl;
    std::cout << join_op->description(DescriptionMode::MultiLine) << std::endl;
    std::cout << "===================== Left Input Table =====================" << std::endl;
    Print::print(input_table_left, PrintFlags::PrintIgnoreChunkBoundaries);
    std::cout << get_table_path(configuration.input_left);
    std::cout << "===================== Right Input Table ====================" << std::endl;
    Print::print(input_table_right, PrintFlags::PrintIgnoreChunkBoundaries);
    std::cout << get_table_path(configuration.input_right);
    std::cout << "==================== Actual Output Table ===================" << std::endl;
    if (join_op->get_output()) {
      Print::print(join_op->get_output(), PrintFlags::PrintIgnoreChunkBoundaries);
    } else {
      std::cout << "No Table produced by the join operator under test" << std::endl;
    }
    std::cout << "=================== Expected Output Table ==================" << std::endl;
    if (join_reference_op->get_output()) {
      Print::print(join_reference_op->get_output(), PrintFlags::PrintIgnoreChunkBoundaries);
    } else {
      std::cout << "No Table produced by the reference join operator" << std::endl;
    }
    std::cout << "======================== Difference ========================" << std::endl;
    std::cout << *table_difference_message << std::endl;
    std::cout << "============================================================" << std::endl;
  };

  try {
    join_reference_op->execute();
    join_op->execute();
  } catch (...) {
    print_configuration_info();
    throw;
  }

  const auto actual_table = join_op->get_output();
  const auto expected_table = join_reference_op->get_output();

  table_difference_message = check_table_equal(actual_table, expected_table, OrderSensitivity::No, TypeCmpMode::Strict,
                                               FloatComparisonMode::AbsoluteDifference);
  if (table_difference_message) {
    print_configuration_info();
    FAIL();
  }
}

// clang-format off
INSTANTIATE_TEST_CASE_P(JoinNestedLoop, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinNestedLoop>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinHash, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinHash>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinSortMerge, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinSortMerge>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinIndex, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinIndex>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinMPSM, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinMPSM>()), );  // NOLINT
// clang-format on

}  // namespace opossum
