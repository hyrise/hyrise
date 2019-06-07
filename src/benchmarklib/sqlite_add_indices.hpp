#pragma once

#include <string>

namespace opossum {

class BenchmarkRunner;

/**
 * Add indexes to SQLite. This is a hack until we support CREATE INDEX ourselves and pass that on to SQLite.
 * Without this, SQLite would never finish.
 */
void add_indices_to_sqlite(std::string schema_file_path, std::string create_indices_file_path,
                           BenchmarkRunner& benchmark_runner);

}  // namespace opossum
