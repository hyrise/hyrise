#include <string>
#include <unordered_map>

#include "types.hpp"
#include "network/opossum.pb.wrapper.h"

namespace opossum {

const std::unordered_map<std::string, proto::ScanType> string_to_proto_scan_type = {
    {"=", opossum::proto::ScanType::OpEquals},        {"!=", opossum::proto::ScanType::OpNotEquals},
    {"<", opossum::proto::ScanType::OpLessThan},      {"<=", opossum::proto::ScanType::OpLessThanEquals},
    {">", opossum::proto::ScanType::OpGreaterThan},   {">=", opossum::proto::ScanType::OpGreaterThanEquals},
    {"BETWEEN", opossum::proto::ScanType::OpBetween}, {"LIKE", opossum::proto::ScanType::OpLike},
};

const std::unordered_map<std::string, ScanType> string_to_scan_type = {
    {"=", opossum::ScanType::OpEquals},        {"!=", opossum::ScanType::OpNotEquals},
    {"<", opossum::ScanType::OpLessThan},      {"<=", opossum::ScanType::OpLessThanEquals},
    {">", opossum::ScanType::OpGreaterThan},   {">=", opossum::ScanType::OpGreaterThanEquals},
    {"BETWEEN", opossum::ScanType::OpBetween}, {"LIKE", opossum::ScanType::OpLike},
};

const std::unordered_map<ScanType, std::string> scan_type_to_string = {
    {opossum::ScanType::OpEquals, "="},        {opossum::ScanType::OpNotEquals, "!="},
    {opossum::ScanType::OpLessThan, "<"},      {opossum::ScanType::OpLessThanEquals, "<="},
    {opossum::ScanType::OpGreaterThan, ">"},   {opossum::ScanType::OpGreaterThanEquals, ">="},
    {opossum::ScanType::OpBetween, "BETWEEN"}, {opossum::ScanType::OpLike, "LIKE"},
};

}  // namespace opossum
