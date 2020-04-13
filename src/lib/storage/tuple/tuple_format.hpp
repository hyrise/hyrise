#pragma once

#include "storage/table_column_definition.hpp"

namespace opposum {

// Categories of sizes a tuple can have
enum class TupleFormatExtent {
  // The size of a tuple depends only on its type (i.e., contains only non-string values)
  Fixed,

  // The size of a tuple varies from tuple to tuple (i.e., contains string values)
  Variable
};

class TupleFormat {
 public:
  static TupleFormat create(const TableColumnDefinitions& column_definitions);



 private:
  //
  std::vector<size_t> external_to_internal_index;

  //

};

}  // namespace opossum