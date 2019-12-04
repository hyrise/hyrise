#pragma once

#include "SQLParser.h"

namespace opossum {

enum class FileType : uint8_t { Csv, Tbl, Binary, Auto };

FileType import_type_to_file_type(hsql::ImportType import_type);

}  // namespace opossum
