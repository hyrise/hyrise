#include "file_type.hpp"

#include "boost/algorithm/string/case_conv.hpp"
#include "utils/assert.hpp"

namespace opossum {

FileType import_type_to_file_type(const hsql::ImportType import_type) {
  switch (import_type) {
    case hsql::ImportType::kImportCSV:
      return FileType::Csv;
    case hsql::ImportType::kImportTbl:
      return FileType::Tbl;
    case hsql::ImportType::kImportBinary:
      return FileType::Binary;
    case hsql::ImportType::kImportAuto:
      return FileType::Auto;
  }
  Fail("Unknown file type.");
}

FileType file_type_from_filename(const std::string& filename) {
  auto extension = std::string{std::filesystem::path{filename}.extension()};
  boost::algorithm::to_lower(extension);
  if (extension == ".csv") {
    return FileType::Csv;
  } else if (extension == ".tbl") {
    return FileType::Tbl;
  } else if (extension == ".bin") {
    return FileType::Binary;
  }
  Fail("Unknown file extension " + extension);
}

}  // namespace opossum
