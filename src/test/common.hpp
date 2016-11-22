#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "../lib/storage/table.hpp"

namespace opossum {

// compares two tables with regard to the schema and content
// but ignores the internal representation (chunk size, column type)
::testing::AssertionResult tablesEqual(const Table &tleft, const Table &tright, bool order_sensitive = false);

// creates a opossum table based from a file
std::shared_ptr<Table> loadTable(std::string file_name, size_t chunk_size);
}  // namespace opossum
