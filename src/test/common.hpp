#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "../lib/storage/table.hpp"

::testing::AssertionResult compareTables(const opossum::Table &tleft, const opossum::Table &tright,
                                         bool sorted = false);

std::shared_ptr<opossum::Table> loadTable(std::string file_name, size_t chunk_size);
