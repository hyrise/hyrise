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

// Using tuple for operator</==
using InputTableKey = std::tuple<
InputSide /* side */,
ChunkOffset /* chunk_size */,
size_t, /* table_size */
InputTableType /* input_table_type */
>;

struct JoinTestRunnerParameter {
  InputTableKey input_left, input_right;
  JoinMode join_mode{};
  DataType data_type_left, data_type_right;
  bool nullable_left{}, nullable_right{};
  PredicateCondition predicate_condition{};
  std::string output_table_name{};
  std::shared_ptr<BaseJoinOperatorFactory> join_operator_factory;
};

std::ostream& operator<<(std::ostream& stream, const JoinTestRunnerParameter& parameter) {
  const auto& [left_side, left_chunk_size, left_table_size, left_input_table_type] = parameter.input_left;
  const auto& [right_side, right_chunk_size, right_table_size, right_input_table_type] = parameter.input_right;

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

  stream << data_type_to_string.left.at(parameter.data_type_left) << " " << data_type_to_string.left.at(parameter.data_type_right) << " ";
  stream << parameter.nullable_left << " " << parameter.nullable_right << " ";
  stream << join_mode_to_string.left.at(parameter.join_mode) << " ";
  stream << predicate_condition_to_string.left.at(parameter.predicate_condition) << " ";
  stream << parameter.output_table_name << " ";

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

class JoinTestRunner : public BaseTestWithParam<JoinTestRunnerParameter> {
 public:
  template<typename JoinOperator>
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
        input_table_type_to_string.right.at(parameter_json["left_reference_segment"].get<std::string>())
      };

      parameter.input_right = {
        InputSide::Right,
        parameter_json["chunk_size"].get<ChunkOffset>(),
        parameter_json["right_table_size"].get<size_t>(),
        input_table_type_to_string.right.at(parameter_json["right_reference_segment"].get<std::string>())
      };

      parameter.join_mode = join_mode_to_string.right.at(parameter_json["join_mode"].get<std::string>());
      parameter.data_type_left = data_type_to_string.right.at(parameter_json["left_data_type"].get<std::string>());
      parameter.data_type_right = data_type_to_string.right.at(parameter_json["right_data_type"].get<std::string>());
      parameter.nullable_left = parameter_json["left_nullable"].get<bool>();
      parameter.nullable_right = parameter_json["right_nullable"].get<bool>();
      parameter.predicate_condition = join_predicate_condition_by_string.at(parameter_json["predicate_condition"].get<std::string>());

      if (parameter_json["swap_tables"].get<bool>()) {
        std::swap(parameter.input_left, parameter.input_right);
        std::swap(parameter.data_type_left, parameter.data_type_right);
        std::swap(parameter.nullable_left, parameter.nullable_right);
      }

      parameter.output_table_name =  parameter_json["output_file_path"].get<std::string>();

      parameter.join_operator_factory = std::make_shared<JoinOperatorFactory<JoinOperator>>();

      if (JoinOperator::supports(parameter.join_mode, parameter.predicate_condition, parameter.data_type_left, parameter.data_type_right)) {
        parameters.emplace_back(parameter);          
      }
    }

    return parameters;
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
INSTANTIATE_TEST_CASE_P(JoinNestedLoop, JoinTestRunner, testing::ValuesIn(JoinTestRunner::load_parameters<JoinNestedLoop>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinHash, JoinTestRunner, testing::ValuesIn(JoinTestRunner::load_parameters<JoinHash>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinSortMerge, JoinTestRunner, testing::ValuesIn(JoinTestRunner::load_parameters<JoinSortMerge>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinIndex, JoinTestRunner, testing::ValuesIn(JoinTestRunner::load_parameters<JoinIndex>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(JoinMPSM, JoinTestRunner, testing::ValuesIn(JoinTestRunner::load_parameters<JoinMPSM>()), );  // NOLINT
// clang-format on

}  // namespace opossum
