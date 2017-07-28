#include <string>
#include <unordered_map>

#include "network/opossum.pb.wrapper.hpp"
#include "types.hpp"

namespace opossum {

const std::unordered_map<std::string, proto::ScanType> string_to_proto_scan_type = {
    {"=", proto::ScanType::OpEquals},        {"!=", proto::ScanType::OpNotEquals},
    {"<", proto::ScanType::OpLessThan},      {"<=", proto::ScanType::OpLessThanEquals},
    {">", proto::ScanType::OpGreaterThan},   {">=", proto::ScanType::OpGreaterThanEquals},
    {"BETWEEN", proto::ScanType::OpBetween}, {"LIKE", proto::ScanType::OpLike},
};

const std::unordered_map<std::string, ScanType> string_to_scan_type = {
    {"=", ScanType::OpEquals},          {"!=", ScanType::OpNotEquals},  {"<", ScanType::OpLessThan},
    {"<=", ScanType::OpLessThanEquals}, {">", ScanType::OpGreaterThan}, {">=", ScanType::OpGreaterThanEquals},
    {"BETWEEN", ScanType::OpBetween},   {"LIKE", ScanType::OpLike},
};

const std::unordered_map<ScanType, std::string> scan_type_to_string = {
    {ScanType::OpEquals, "="},          {ScanType::OpNotEquals, "!="},  {ScanType::OpLessThan, "<"},
    {ScanType::OpLessThanEquals, "<="}, {ScanType::OpGreaterThan, ">"}, {ScanType::OpGreaterThanEquals, ">="},
    {ScanType::OpBetween, "BETWEEN"},   {ScanType::OpLike, "LIKE"},
};

const std::unordered_map<const JoinMode, std::string> join_mode_to_string = {
    {JoinMode::Cross, "Cross"}, {JoinMode::Inner, "Inner"}, {JoinMode::Left, "Left"}, {JoinMode::Natural, "Natural"},
    {JoinMode::Outer, "Outer"}, {JoinMode::Right, "Right"}, {JoinMode::Self, "Self"},
};

}  // namespace opossum
