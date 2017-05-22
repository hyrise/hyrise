#include "table_scan.hpp"

#include <map>
#include <memory>
#include <string>

#include "resolve_type.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<AbstractOperator> in, const std::string& column_name, const std::string& op,
                     const AllParameterVariant value, const optional<AllTypeVariant> value2)
    : AbstractReadOnlyOperator(in), _column_name(column_name), _op(op), _value(value), _value2(value2) {}

const std::string TableScan::name() const { return "TableScan"; }

uint8_t TableScan::num_in_tables() const { return 1; }

uint8_t TableScan::num_out_tables() const { return 1; }

std::shared_ptr<const Table> TableScan::on_execute() {
  _impl = make_unique_by_column_type<AbstractReadOnlyOperatorImpl, TableScanImpl>(
      input_table_left()->column_type(input_table_left()->column_id_by_name(_column_name)), _input_left, _column_name,
      _op, _value, _value2);
  return _impl->on_execute();
}

std::string& TableScan::replace_all(std::string& str, const std::string& old_value, const std::string& new_value) {
  std::string::size_type pos = 0;
  while ((pos = str.find(old_value, pos)) != std::string::npos) {
    str.replace(pos, old_value.size(), new_value);
    pos += new_value.size() - old_value.size() + 1;
  }
  return str;
}

std::map<std::string, std::string> TableScan::extract_character_ranges(std::string& str) {
  std::map<std::string, std::string> ranges;

  int rangeID = 0;
  std::string::size_type startPos = 0;
  std::string::size_type endPos = 0;

  while ((startPos = str.find("[", startPos)) != std::string::npos &&
         (endPos = str.find("]", startPos + 1)) != std::string::npos) {
    std::stringstream ss;
    ss << "[[" << rangeID << "]]";
    std::string chars = str.substr(startPos + 1, endPos - startPos - 1);
    str.replace(startPos, chars.size() + 2, ss.str());
    rangeID++;
    startPos += ss.str().size();

    replace_all(chars, "[", "\\[");
    replace_all(chars, "]", "\\]");
    ranges[ss.str()] = "[" + chars + "]";
  }

  int open = 0;
  std::string::size_type searchPos = 0;
  startPos = 0;
  endPos = 0;
  do {
    startPos = str.find("[", searchPos);
    endPos = str.find("]", searchPos);

    if (startPos == std::string::npos && endPos == std::string::npos) break;

    if (startPos < endPos || endPos == std::string::npos) {
      open++;
      searchPos = startPos + 1;
    } else {
      if (open <= 0) {
        str.replace(endPos, 1, "\\]");
        searchPos = endPos + 2;
      } else {
        open--;
        searchPos = endPos + 1;
      }
    }
  } while (searchPos < str.size());
  return ranges;
}

/*
 * converts a SQL LIKE to a regex
 * copied from http://stackoverflow.com/questions/34897842/convert-sql-like-expression-to-regex-with-c-or-qt
 * */

std::string TableScan::sqllike_to_regex(std::string sqllike) {
  replace_all(sqllike, ".", "\\.");
  replace_all(sqllike, "^", "\\^");
  replace_all(sqllike, "$", "\\$");
  replace_all(sqllike, "+", "\\+");
  replace_all(sqllike, "?", "\\?");
  replace_all(sqllike, "(", "\\(");
  replace_all(sqllike, ")", "\\)");
  replace_all(sqllike, "{", "\\{");
  replace_all(sqllike, "}", "\\}");
  replace_all(sqllike, "\\", "\\\\");
  replace_all(sqllike, "|", "\\|");
  replace_all(sqllike, ".", "\\.");
  replace_all(sqllike, "*", "\\*");
  std::map<std::string, std::string> ranges = extract_character_ranges(sqllike);  // Escapes [ and ] where necessary
  replace_all(sqllike, "%", ".*");
  replace_all(sqllike, "_", ".");
  for (auto& range : ranges) {
    replace_all(sqllike, range.first, range.second);
  }
  return "^" + sqllike + "$";
}

}  // namespace opossum
