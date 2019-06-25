#pragma once

#include "types.hpp"

namespace opossum {

const std::vector<std::optional<ColumnID>> column_ids_after_pruning(const size_t& original_table_column_count,
                                                                    const std::vector<ColumnID>& pruned_column_ids);

}
