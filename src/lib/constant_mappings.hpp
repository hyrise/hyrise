#include <string>
#include <unordered_map>

#include "network/opossum.pb.wrapper.hpp"
#include "types.hpp"

namespace opossum {

extern const std::unordered_map<std::string, proto::ScanType> string_to_proto_scan_type;
extern const std::unordered_map<std::string, ScanType> string_to_scan_type;
extern const std::unordered_map<ScanType, std::string> scan_type_to_string;
extern const std::unordered_map<const ExpressionType, std::string> expression_type_to_string;
extern const std::unordered_map<std::string, AggregateFunction> string_to_aggregate_function;
extern const std::unordered_map<const JoinMode, std::string> join_mode_to_string;

}  // namespace opossum
