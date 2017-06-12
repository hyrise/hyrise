#include "statistics.hpp"

#include <memory>
#include <string>

namespace opossum {

size_t Statistics::predicate_result_size(const std::shared_ptr<Table> table, const std::string &column_name,
                                         const std::string &op, const AllParameterVariant value,
                                         const optional<AllTypeVariant> value2) {
  return 50;
}

}  // namespace opossum
