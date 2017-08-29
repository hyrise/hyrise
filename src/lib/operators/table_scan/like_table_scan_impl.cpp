#include "like_table_scan_impl.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "table_scan_main_loop.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/constant_value_iterable.hpp"
#include "storage/iterables/value_column_iterable.hpp"
#include "storage/value_column.hpp"

namespace opossum {

LikeTableScanImpl::LikeTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                     const std::string &right_wildcard)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, ScanType::OpLike}, _right_wildcard{right_wildcard} {
  // convert the given SQL-like search term into a c++11 regex to use it for the actual matching
  auto regex_string = _sqllike_to_regex(_right_wildcard);
  _regex = std::regex{regex_string, std::regex_constants::icase};  // case insentivity
}

void LikeTableScanImpl::handle_value_column(BaseColumn &base_column,
                                            std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto &matches_out = context->_matches_out;
  const auto chunk_id = context->_chunk_id;

  auto &left_column = static_cast<const ValueColumn<std::string> &>(base_column);

  auto left_iterable = ValueColumnIterable<std::string>{left_column, context->_mapped_chunk_offsets.get()};
  auto right_iterable = ConstantValueIterable<std::regex>{_regex};

  static const auto regex_comparator = [](const std::string& str, const std::regex& regex) {
    return std::regex_match(str, regex);
  };

  left_iterable.get_iterators([&](auto left_it, auto left_end) {
    right_iterable.get_iterators([&](auto right_it, auto right_end) {
      TableScanMainLoop{}(regex_comparator, left_it, left_end, right_it, chunk_id, matches_out);
    });
  });
}

void LikeTableScanImpl::handle_dictionary_column(BaseColumn &base_column,
                                                 std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto &matches_out = context->_matches_out;
  const auto chunk_id = context->_chunk_id;

  const auto &left_column = static_cast<const DictionaryColumn<std::string> &>(base_column);

  const auto dictionary_matches = _find_matches_in_dictionary(*left_column.dictionary());

  const auto match_count =
      static_cast<size_t>(std::count(dictionary_matches.cbegin(), dictionary_matches.cend(), true));

  const auto &attribute_vector = *left_column.attribute_vector();
  auto attribute_vector_iterable = AttributeVectorIterable{attribute_vector, context->_mapped_chunk_offsets.get()};

  if (match_count == dictionary_matches.size()) {
    attribute_vector_iterable.get_iterators([&](auto left_it, auto left_end) {
      for (; left_it != left_end; ++left_it) {
        const auto left = *left_it;

        if (left.is_null()) continue;

        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    });
  }

  if (match_count == 0u) {
    return;
  }

  attribute_vector_iterable.get_iterators([&](auto left_it, auto left_end) {
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;

      if (left.is_null()) continue;

      if (dictionary_matches[left.value()]) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  });
}

std::vector<bool> LikeTableScanImpl::_find_matches_in_dictionary(const std::vector<std::string> &dictionary) {
  auto dictionary_matches = std::vector<bool>{};
  dictionary_matches.reserve(dictionary.size());

  for (const auto &value : dictionary) {
    dictionary_matches.push_back(std::regex_match(value, _regex));
  }

  return dictionary_matches;
}

std::string &LikeTableScanImpl::_replace_all(std::string &str, const std::string &old_value,
                                             const std::string &new_value) {
  std::string::size_type pos = 0;
  while ((pos = str.find(old_value, pos)) != std::string::npos) {
    str.replace(pos, old_value.size(), new_value);
    pos += new_value.size() - old_value.size() + 1;
  }
  return str;
}

std::map<std::string, std::string> LikeTableScanImpl::_extract_character_ranges(std::string &str) {
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

    _replace_all(chars, "[", "\\[");
    _replace_all(chars, "]", "\\]");
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

std::string LikeTableScanImpl::_sqllike_to_regex(std::string sqllike) {
  _replace_all(sqllike, ".", "\\.");
  _replace_all(sqllike, "^", "\\^");
  _replace_all(sqllike, "$", "\\$");
  _replace_all(sqllike, "+", "\\+");
  _replace_all(sqllike, "?", "\\?");
  _replace_all(sqllike, "(", "\\(");
  _replace_all(sqllike, ")", "\\)");
  _replace_all(sqllike, "{", "\\{");
  _replace_all(sqllike, "}", "\\}");
  _replace_all(sqllike, "\\", "\\\\");
  _replace_all(sqllike, "|", "\\|");
  _replace_all(sqllike, ".", "\\.");
  _replace_all(sqllike, "*", "\\*");
  std::map<std::string, std::string> ranges = _extract_character_ranges(sqllike);  // Escapes [ and ] where necessary
  _replace_all(sqllike, "%", ".*");
  _replace_all(sqllike, "_", ".");
  for (auto &range : ranges) {
    _replace_all(sqllike, range.first, range.second);
  }
  return "^" + sqllike + "$";
}

}  // namespace opossum
