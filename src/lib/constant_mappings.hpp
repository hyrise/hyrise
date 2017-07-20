#include <string>
#include <unordered_map>

#include "network/opossum.pb.wrapper.hpp"
#include "sql/Expr.h"
#include "types.hpp"

namespace opossum {

extern const std::unordered_map<std::string, proto::ScanType> string_to_proto_scan_type;
extern const std::unordered_map<std::string, ScanType> string_to_scan_type;
extern const std::unordered_map<ScanType, std::string> scan_type_to_string;
extern const std::unordered_map<const ExpressionType, std::string> expression_type_to_string;
extern const std::unordered_map<const hsql::OperatorType, const ExpressionType> operator_type_to_expression_type;

}  // namespace opossum
