#pragma once

#include <cstdlib>
#include <map>

namespace opossum {

/**
 * Contains all supported TPCH queries. Use ordered map to have  by query id.
 */
extern const std::map<size_t, const char*> tpch_queries;

}  // namespace opossum
