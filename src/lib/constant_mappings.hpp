#include <boost/bimap.hpp>
#include <string>
#include <unordered_map>

#include "network/opossum.pb.wrapper.hpp"
#include "sql/Expr.h"
#include "types.hpp"

namespace opossum {

extern const std::unordered_map<std::string, proto::ScanType> string_to_proto_scan_type;
extern const boost::bimap<ScanType, std::string> scan_type_to_string;
extern const std::unordered_map<ExpressionType, std::string> expression_type_to_string;
extern const std::unordered_map<hsql::OperatorType, ExpressionType> operator_type_to_expression_type;
extern const std::unordered_map<ExpressionType, std::string> expression_type_to_operator_string;

}  // namespace opossum
