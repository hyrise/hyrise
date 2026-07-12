#include "operators/aggregate_dyod/output_columns.hpp"

#include <cstddef>

#include "storage/table_column_definition.hpp"
#include "utils/assert.hpp"

// The methods below are Fail() stubs so the key schema's tests compile and link; the actual implementations are added
// test-driven in subsequent steps. Fail()-only bodies trip -Wmissing-noreturn; the real implementations will return.
#ifdef __clang__
#pragma clang diagnostic ignored "-Wmissing-noreturn"
#endif

namespace hyrise {

OutputColumns::OutputColumns(const TableColumnDefinitions& /*output_column_definitions*/, size_t /*seal_threshold*/)
    : _seal_threshold{0} {
  static_cast<void>(_seal_threshold);
  Fail("OutputColumns is not implemented yet.");
}

AbstractOutputColumn& OutputColumns::column(size_t /*output_column_index*/) {
  Fail("OutputColumns::column is not implemented yet.");
}

void OutputColumns::maybe_seal() {
  Fail("OutputColumns::maybe_seal is not implemented yet.");
}

void OutputColumns::seal_all() {
  Fail("OutputColumns::seal_all is not implemented yet.");
}

}  // namespace hyrise
