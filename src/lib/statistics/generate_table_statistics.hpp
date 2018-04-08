#pragma once

#include "table_statistics2.hpp"

namespace opossum {

class Table;

/**
 * Generate statistics about a Table by analysing its entire data. This may be slow, use with caution.
 */
TableStatistics2 generate_table_statistics(const Table& table);

}  // namespace opossum
