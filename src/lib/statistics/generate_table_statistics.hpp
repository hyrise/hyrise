#pragma once

#include <memory>
#include <unordered_set>

#include "table_statistics.hpp"

namespace opossum {

class Table;

/**
 * Generate statistics about a Table by analysing its entire data. This may be slow, use with caution.
 */
TableStatistics generate_table_statistics(const Table& table);

}  // namespace opossum
