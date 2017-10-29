#include "ast_utils.hpp"

#include <map>

#include "boost/variant.hpp"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {


ColumnIDMapping ast_generate_column_id_mapping(const ColumnOrigins &column_origins_a,
                                               const ColumnOrigins &column_origins_b) {
  DebugAssert(column_origins_a.size() == column_origins_b.size(), "Params must be shuffled set of each other");

  ColumnIDMapping output_mapping(column_origins_a.size(), INVALID_COLUMN_ID);
  std::map<ColumnOrigin, size_t> column_origin_to_input_idx;

  for (size_t column_idx = 0; column_idx < column_origins_a.size(); ++column_idx) {
    const auto result = column_origin_to_input_idx.emplace(column_origins_a[column_idx], column_idx);
    Assert(result.second, "The same column origin appears multiple times, can't create unambiguous mapping");
  }

  for (auto column_idx = ColumnID{0}; column_idx < column_origins_b.size(); ++column_idx) {
    auto iter = column_origin_to_input_idx.find(column_origins_b[column_idx]);
    DebugAssert(iter != column_origin_to_input_idx.end(), "This shouldn't happen.");
    output_mapping[iter->second] = column_idx;
  }

  return output_mapping;
}

}  // namespace opossum