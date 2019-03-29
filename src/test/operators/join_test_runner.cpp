#include "base_test.hpp"

#include <fstream>

#include "json.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/join_nested_loop.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;  // NOLINT

namespace {

using namespace opossum; // NOLINT

enum class InputSide { Left, Right };

// Using tuple for operator</==
using InputTableKey = std::tuple<
InputSide /* side */,
ChunkOffset /* chunk_size */,
size_t, /* table_size */
bool /* reference_segment */
>;

struct JoinTestRunnerParameter {
  InputTableKey input_left, input_right;
  JoinMode join_mode{};
  DataType data_type_left, data_type_right;
  bool nullable_left{}, nullable_right{};
  PredicateCondition predicate_condition{};
  std::string output_table_name{};
};

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

class JoinTestRunner : public BaseTestWithParam<JoinTestRunnerParameter> {
 public:
  static std::vector<JoinTestRunnerParameter> load_parameters() {
    auto parameters_file = std::ifstream{"resources/test_data/tbl/join_operators/generated_tables/join_configurations.json"};

    auto parameters_json = nlohmann::json{};
    parameters_file >> parameters_json;

    auto parameters = std::vector<JoinTestRunnerParameter>{};

    for (const auto& parameter_json : parameters_json) {
      auto parameter = JoinTestRunnerParameter{};

      parameter.input_left = {
        InputSide::Left,
        parameter_json["chunk_size"].get<ChunkOffset>(),
        parameter_json["left_table_size"].get<size_t>(),
        parameter_json["left_reference_segment"].get<std::string>() == "Yes"
      };

      parameter.input_right = {
        InputSide::Left,
        parameter_json["chunk_size"].get<ChunkOffset>(),
        parameter_json["right_table_size"].get<size_t>(),
        parameter_json["right_reference_segment"].get<std::string>() == "Yes"
      };

      parameter.join_mode = join_mode_to_string.right.at(parameter_json["join_mode"].get<std::string>());
      parameter.data_type_left = data_type_to_string.right.at(parameter_json["left_data_type"].get<std::string>());
      parameter.data_type_right = data_type_to_string.right.at(parameter_json["right_data_type"].get<std::string>());
      parameter.nullable_left = parameter_json["left_null"].get<bool>();
      parameter.nullable_right = parameter_json["right_null"].get<bool>();
      parameter.predicate_condition = join_predicate_condition_by_string.at(parameter_json["predicate_condition"].get<std::string>());

      if (parameter_json["swap_tables"].get<bool>()) {
        std::swap(parameter.input_left, parameter.input_right);
        std::swap(parameter.data_type_left, parameter.data_type_right);
        std::swap(parameter.nullable_left, parameter.nullable_right);
      }

      parameter.output_table_name =  parameter_json["output_file_path"].get<std::string>();

      parameters.emplace_back(parameter);
    }

    return parameters;
  }

  static std::shared_ptr<Table> get_table(const InputTableKey& key) {
    auto input_table_iter = input_tables.find(key);
    if (input_table_iter == input_tables.end()) {
      const auto& [side, chunk_size, table_size, as_reference_segments] = key;

      const auto side_str = side == InputSide::Left ? "left" : "right";
      const auto table_size_str = std::to_string(table_size);

      const auto table_path = "resources/test_data/tbl/join_operators/generated_tables/join_table_"s + side_str + "_" + table_size_str + ".tbl";

      const auto table = load_table(table_path, chunk_size);
      // TODO(moritz) Handle AsReferenceSegments

      input_table_iter = input_tables.emplace(key, table).first;
    }

    return input_table_iter->second;
  }

  static inline std::map<InputTableKey, std::shared_ptr<Table>> input_tables;
};

TEST_P(JoinTestRunner, TestJoin) {
  const auto parameter = GetParam();

  const auto input_table_left = get_table(parameter.input_left);
  const auto input_table_right = get_table(parameter.input_right);
  const auto expected_output_table = load_table("resources/test_data/tbl/join_operators/generated_tables/"s + parameter.output_table_name);

  const auto input_op_left = std::make_shared<TableWrapper>(input_table_left);
  const auto input_op_right = std::make_shared<TableWrapper>(input_table_right);

  const auto column_id_left = ColumnID{static_cast<ColumnID::base_type>(2 * data_type_order.at(parameter.data_type_left) + (parameter.nullable_left ? 1 : 0))};
  const auto column_id_right = ColumnID{static_cast<ColumnID::base_type>(2 * data_type_order.at(parameter.data_type_right) + (parameter.nullable_right ? 1 : 0))};

  const auto primary_predicate = OperatorJoinPredicate{
    {column_id_left, column_id_right}, parameter.predicate_condition};

  const auto join_op = std::make_shared<JoinNestedLoop>(input_op_left, input_op_right, parameter.join_mode, primary_predicate);

  input_op_left->execute();
  input_op_right->execute();
  join_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(join_op->get_output(), expected_output_table);
}

INSTANTIATE_TEST_CASE_P(
JoinTestRunnerInstance, JoinTestRunner, testing::ValuesIn(JoinTestRunner::load_parameters()), );  // NOLINT(whitespace/parens)  // NOLINT

}  // namespace opossum
