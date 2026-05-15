#pragma once

#include <string>

#include "SQLParser.h"

namespace hyrise {

enum class FileType : uint8_t { Csv, Tbl, Binary, Auto };

FileType import_type_to_file_type(const hsql::ImportType import_type);

FileType file_type_from_filename(const std::string& filename);

}  // namespace hyrise
