#include "base_test.hpp"

#include <fstream>

#include "json.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/join_index.hpp"
#include "operators/join_mpsm.hpp"
#include "utils/load_table.hpp"
#include "utils/make_bimap.hpp"

using namespace std::string_literals;  // NOLINT

namespace {

using namespace opossum; // NOLINT

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
  virtual std::shared_ptr<AbstractJoinOperator> create_operator(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
           const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
           const std::vector<OperatorJoinPredicate>& secondary_predicates = {}) = 0;
};

template<typename JoinOperator>
class JoinOperatorFactory : public BaseJoinOperatorFactory{
public:
  std::shared_ptr<AbstractJoinOperator> create_operator(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
           const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
           const std::vector<OperatorJoinPredicate>& secondary_predicates) override {
    return std::make_shared<JoinOperator>(left, right, mode, primary_predicate, secondary_predicates);
  }
};

const boost::bimap<InputTableType, std::string> input_table_type_to_string =
    make_bimap<InputTableType, std::string>({
        {InputTableType::Data, "No"},
        {InputTableType::SharedPosList, "Yes"},
        {InputTableType::IndividualPosLists, "Join"},
    });

struct InputTableKey {
  InputSide side{};
  ChunkOffset chunk_size{};
  size_t table_size{};
  InputTableType input_table_type{};

  auto to_tuple() const {
    return std::tie(side, chunk_size, table_size, input_table_type);
  }
};

bool operator<(const InputTableKey& l, const InputTableKey& r) {
  return l.to_tuple() < r.to_tuple();
}

struct JoinTestConfiguration {
  InputTableKey input_left;
  InputTableKey input_right;
  JoinMode join_mode{JoinMode::Inner};
  DataType data_type_left{DataType::Int};
  DataType data_type_right{DataType::Int};
  bool nullable_left{false};
  bool nullable_right{false};
  PredicateCondition predicate_condition{PredicateCondition::Equals};
  std::string output_table_name{};
  std::shared_ptr<BaseJoinOperatorFactory> join_operator_factory;
};

std::ostream& operator<<(std::ostream& stream, const JoinTestConfiguration& configuration) {
  const auto& [left_side, left_chunk_size, left_table_size, left_input_table_type] = configuration.input_left;
  const auto& [right_side, right_chunk_size, right_table_size, right_input_table_type] = configuration.input_right;

  stream << "LeftInput: [";
  stream << (left_side == InputSide::Left ? "left" : "right") << " ";
  stream << left_chunk_size << " ";
  stream << left_table_size << " ";
  stream << input_table_type_to_string.left.at(left_input_table_type);
  stream << "] ";

  stream << "RightInput: [";
  stream << (right_side == InputSide::Left ? "left" : "right") << " ";
  stream << right_chunk_size << " ";
  stream << right_table_size << " ";
  stream << input_table_type_to_string.left.at(right_input_table_type);
  stream << "]";

  stream << data_type_to_string.left.at(configuration.data_type_left) << " " << data_type_to_string.left.at(configuration.data_type_right) << " ";
  stream << configuration.nullable_left << " " << configuration.nullable_right << " ";
  stream << join_mode_to_string.left.at(configuration.join_mode) << " ";
  stream << predicate_condition_to_string.left.at(configuration.predicate_condition) << " ";
  stream << configuration.output_table_name << " ";

  return stream;
}

const std::unordered_map<std::string, PredicateCondition> join_predicate_condition_by_string{
                                            {"Equals", PredicateCondition::Equals},
                                            {"NotEquals", PredicateCondition::NotEquals},
                                            {"LessThan", PredicateCondition::LessThan},
                                            {"LessThanEquals", PredicateCondition::LessThanEquals},
                                            {"GreaterThan", PredicateCondition::GreaterThan},
                                            {"GreaterThanEquals", PredicateCondition::GreaterThanEquals}};

const std::unordered_map<DataType, size_t> data_type_order = {
  {DataType::Int, 0u},
  {DataType::Float, 1u},
  {DataType::Double, 2u},
  {DataType::Long, 3u},
  {DataType::String, 4u},
};

}  // namespace

namespace opossum {

class JoinTestRunner : public BaseTestWithParam<JoinTestConfiguration> {
 public:
  template<typename JoinOperator>
  static std::vector<JoinTestConfiguration> create_configurations() {
    auto configurations = std::vector<JoinTestConfiguration>{};

    auto all_data_types = std::vector<DataType>{};
    hana::for_each(data_type_pairs, [&](auto pair) {
      const DataType d = hana::first(pair);
      all_data_types.emplace_back(d);
    });

    const auto all_predicate_conditions = std::vector{PredicateCondition::Equals, PredicateCondition::NotEquals, PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals, PredicateCondition::LessThan, PredicateCondition::LessThanEquals, };
    const auto all_join_modes = std::vector{JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter, JoinMode::Semi, JoinMode::AntiNullAsFalse, JoinMode::AntiNullAsTrue};
    const auto all_left_table_sizes = std::vector{0u, 10u, 15u};
    const auto all_right_table_sizes = std::vector{0u, 10u, 15u};
    const auto all_left_nulls = std::vector{true, false};
    const auto all_right_nulls = std::vector{true, false};
    const auto all_chunk_sizes = std::vector{1u, 3u, 10u};
    // const auto all_mpj = std::vector{1u, 2u};
    const auto all_swap_tables = std::vector{true, false};
    const auto all_input_table_types = std::vector{InputTableType::Data,
                                                   InputTableType::IndividualPosLists,
                                                   InputTableType::SharedPosList};

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
      {},
      {}
    };
    // clang-format on


    const auto add_configuration_if_supported = [&](const auto& configuration) {
      if (JoinOperator::supports(configuration.join_mode, configuration.predicate_condition, configuration.data_type_left, configuration.data_type_right)) {
        configurations.emplace_back(configuration);
      }
    };

    for (const auto& data_type_left : all_data_types) {
      for (const auto &data_type_right : all_data_types) {
        if ((data_type_left == DataType::String) != (data_type_right == DataType::String)) {
          continue;
        }

        auto join_test_configuration = default_configuration;
        join_test_configuration.data_type_left = data_type_left;
        join_test_configuration.data_type_right = data_type_right;

        add_configuration_if_supported(join_test_configuration);
      }
    }

    for (const auto& predicate_condition : all_predicate_conditions) {
      for (const auto left_table_size : all_left_table_sizes) {
        for (const auto right_table_size : all_right_table_sizes) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.predicate_condition = predicate_condition;
          join_test_configuration.input_left.table_size = left_table_size;
          join_test_configuration.input_right.table_size = right_table_size;

          add_configuration_if_supported(join_test_configuration);
        }
      }
    }

    for (const auto& left_table_size : all_left_table_sizes) {
      for (const auto &right_table_size : all_right_table_sizes) {
        for (const auto &chunk_size : all_chunk_sizes) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.input_left.table_size = left_table_size;
          join_test_configuration.input_right.table_size = right_table_size;
          join_test_configuration.input_left.chunk_size = chunk_size;
          join_test_configuration.input_right.chunk_size = chunk_size;

          add_configuration_if_supported(join_test_configuration);
        }
      }
    }

    for (const auto& join_mode : all_join_modes) {
      for (const auto left_null : all_left_nulls) {
        for (const auto right_null : all_right_nulls) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.join_mode = join_mode;
          join_test_configuration.nullable_left = left_null;
          join_test_configuration.nullable_right = right_null;

          add_configuration_if_supported(join_test_configuration);
        }
      }
    }

    for (const auto& join_mode : all_join_modes) {
      for (const auto &left_table_size : all_left_table_sizes) {
        for (const auto &right_table_size : all_right_table_sizes) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.join_mode = join_mode;
          join_test_configuration.input_left.table_size = left_table_size;
          join_test_configuration.input_right.table_size = right_table_size;

          add_configuration_if_supported(join_test_configuration);
        }
      }
    }

    for (const auto& predicate_condition : all_predicate_conditions) {
      for (const auto &join_mode : all_join_modes) {
        for (const auto swap_table : all_swap_tables) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.join_mode = join_mode;
          join_test_configuration.predicate_condition = predicate_condition;

          if (swap_table) {
            std::swap(join_test_configuration.input_left, join_test_configuration.input_right);
            std::swap(join_test_configuration.data_type_left, join_test_configuration.data_type_right);
            std::swap(join_test_configuration.nullable_left, join_test_configuration.nullable_right);
          }

          add_configuration_if_supported(join_test_configuration);
        }
      }
    }

    for (const auto& left_input_table_type : all_input_table_types) {
      for (const auto &right_input_table_type : all_input_table_types) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.input_left.input_table_type = left_input_table_type;
        join_test_configuration.input_right.input_table_type = right_input_table_type;

        add_configuration_if_supported(join_test_configuration);
      }
    }

//    for (const auto& join_mode : all_join_modes) {
//      for (const auto &mpj : all_mpj) {
//        auto join_test_configuration = JoinTestConfiguration{};
//        join_test_configuration.join_mode = join_mode
//        join_test_configuration.mpj = mpj
//
//        add_configuration_if_supported(join_test_configuration)
//      }
//    }

    return configurations;
  }

  static std::shared_ptr<Table> get_table(const InputTableKey& key) {
    auto input_table_iter = input_tables.find(key);
    if (input_table_iter == input_tables.end()) {
      const auto& [side, chunk_size, table_size, input_table_type] = key;

      const auto side_str = side == InputSide::Left ? "left" : "right";
      const auto table_size_str = std::to_string(table_size);

      const auto table_path = "resources/test_data/tbl/join_operators/generated_tables/join_table_"s + side_str + "_" + table_size_str + ".tbl";

      auto table = load_table(table_path, chunk_size);

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
  const auto parameter = GetParam();

  SCOPED_TRACE(parameter);

  const auto input_table_left = get_table(parameter.input_left);
  const auto input_table_right = get_table(parameter.input_right);
  const auto expected_output_table = load_table("resources/test_data/tbl/join_operators/generated_tables/"s + parameter.output_table_name);

  const auto input_op_left = std::make_shared<TableWrapper>(input_table_left);
  const auto input_op_right = std::make_shared<TableWrapper>(input_table_right);

  const auto column_id_left = ColumnID{static_cast<ColumnID::base_type>(2 * data_type_order.at(parameter.data_type_left) + (parameter.nullable_left ? 1 : 0))};
  const auto column_id_right = ColumnID{static_cast<ColumnID::base_type>(2 * data_type_order.at(parameter.data_type_right) + (parameter.nullable_right ? 1 : 0))};

  const auto primary_predicate = OperatorJoinPredicate{
    {column_id_left, column_id_right}, parameter.predicate_condition};

  const auto join_op = parameter.join_operator_factory->create_operator(input_op_left, input_op_right, parameter.join_mode, primary_predicate, std::vector<OperatorJoinPredicate>{});

  input_op_left->execute();
  input_op_right->execute();
  join_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(join_op->get_output(), expected_output_table);
}

// clang-format off
INSTANTIATE_TEST_CASE_P(JoinNestedLoop, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinNestedLoop>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinHash, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinHash>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinSortMerge, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinSortMerge>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinIndex, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinIndex>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinMPSM, JoinTestRunner, testing::ValuesIn(JoinTestRunner::create_configurations<JoinMPSM>()), );  // NOLINT
// clang-format on

}  // namespace opossum
