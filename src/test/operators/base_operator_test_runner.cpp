#include "base_operator_test_runner.hpp"

namespace opossum {

std::unordered_map<InputTableType, std::string> input_table_type_to_string{
    {InputTableType::Data, "Data"},
    {InputTableType::SharedPosList, "SharedPosList"},
    {InputTableType::IndividualPosLists, "IndividualPosLists"}};

bool operator<(const InputTableConfiguration& l, const InputTableConfiguration& r) {
  return l.to_tuple() < r.to_tuple();
}

bool operator==(const InputTableConfiguration& l, const InputTableConfiguration& r) {
  return l.to_tuple() == r.to_tuple();
}

}  // namespace opossum
