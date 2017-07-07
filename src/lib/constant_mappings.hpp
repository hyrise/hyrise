#include <string>
#include <unordered_map>

namespace opossum {

  const std::unordered_map<std::string, const proto::ScanType> op_string_to_scan_type = {
    {"=", opossum::proto::ScanType::OpEquals},
    {"!=", opossum::proto::ScanType::OpNotEquals},

    {"<", opossum::proto::ScanType::OpLessThan},
    {"<=", opossum::proto::ScanType::OpLessThanEquals},

    {">", opossum::proto::ScanType::OpGreaterThan},
    {">=", opossum::proto::ScanType::OpGreaterThanEquals},

    {"BETWEEN", opossum::proto::ScanType::OpBetween},
    {"LIKE", opossum::proto::ScanType::OpLike},
  };

}  // namespace opossum
