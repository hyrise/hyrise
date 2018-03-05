#pragma once

#include <cstdlib>

namespace opossum {

constexpr size_t NUM_TPCH_QUERIES = 22;
constexpr size_t NUM_SUPPORTED_TPCH_QUERIES = 7;

extern const char* tpch_queries[NUM_TPCH_QUERIES];

/**
 * Indicates whether the query with a specific index is considered to be supported by Hyrise
 */
extern size_t tpch_supported_queries[NUM_SUPPORTED_TPCH_QUERIES];
}  // namespace opossum
