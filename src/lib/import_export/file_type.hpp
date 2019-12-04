#pragma once

#include "utils/assert.hpp"

#include "SQLParser.h"

namespace opossum {

enum class FileType { Csv, Tbl, Binary, Auto };

// FileType import_type_to_file_type(hsql::ImportType import_type);

inline FileType import_type_to_file_type(hsql::ImportType import_type) {
  switch (import_type) {
    case hsql::ImportType::kImportCSV:
      return FileType::Csv;
    case hsql::ImportType::kImportTbl:
      return FileType::Tbl;
    default:
      Fail("Unknown Import type");
  }
}

}  // namespace opossum
