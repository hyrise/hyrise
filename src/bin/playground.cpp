#include <cmath>
#include <ctime>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <locale>
#include <memory>
#include <random>
#include <regex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "boost/algorithm/string/replace.hpp"

#include "constant_mappings.hpp"
#include "types.hpp"

#include "expression/evaluation/like_matcher.hpp"
#include "import_export/csv_meta.hpp"
#include "import_export/csv_parser.hpp"
#include "operators/import_binary.hpp"
#include "operators/import_csv.hpp"
#include "statistics/base_column_statistics.hpp"
#include "statistics/chunk_statistics/counting_quotient_filter.hpp"
#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "statistics/chunk_statistics/range_filter.hpp"
#include "statistics/generate_column_statistics.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/table.hpp"
#include "utils/filesystem.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

template <typename Clock, typename Duration>
std::ostream& operator<<(std::ostream& stream, const std::chrono::time_point<Clock, Duration>& time_point) {
  const time_t time = Clock::to_time_t(time_point);
#if __GNUC__ > 4 || ((__GNUC__ == 4) && __GNUC_MINOR__ > 8 && __GNUC_REVISION__ > 1)
  // Maybe the put_time will be implemented later?
  struct tm tm;
  localtime_r(&time, &tm);
  return stream << std::put_time(&tm, "%c");  // Print standard date&time
#else
  char buffer[26];
  ctime_r(&time, buffer);
  buffer[24] = '\0';  // Removes the newline that is added
  return stream << buffer;
#endif
}

void log(const std::string& str) { std::cout << std::chrono::system_clock::now() << " " << str << std::endl; }

/**
 * Converts string to T.
 */
template <typename T>
T str2T(const std::string& param) {
  if constexpr (std::is_same_v<T, std::string>) {
    return param;
  }

  if constexpr (std::is_same_v<T, float>) {
    return stof(param);
  }

  if constexpr (std::is_same_v<T, double>) {
    return stod(param);
  }

  if constexpr (std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t>) {
    return stol(param);
  }

  if constexpr (std::is_same_v<T, int64_t>) {
    return stoll(param);
  }

  if constexpr (std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t>) {
    return stoul(param);
  }

  if constexpr (std::is_same_v<T, uint64_t>) {
    return stoull(param);
  }

  Fail("Unknown conversion method.");
}

/**
 * Converts char pointer to T.
 */
template <typename T>
T str2T(char* param) {
  return str2T<T>(std::string{param});
}

/**
 * Returns the value in argv after option, or default_value if option is not found or there is no value after it.
 * If no default value is given and the search does not yield anything, the method fails as well.
 */
template <typename T>
T get_cmd_option(char** begin, char** end, const std::string& option,
                 const std::optional<T>& default_value = std::nullopt) {
  const auto iter = std::find(begin, end, option);
  if (iter == end || iter + 1 == end) {
    if (static_cast<bool>(default_value)) {
      return *default_value;
    }

    Fail("Option '" + option + "' was not specified, and no default was given.");
  }

  return str2T<T>(*(iter + 1));
}

/**
 * Similar to get_cmd_option, but interprets the value of the option as a comma-delimited list.
 * Returns a vector of T values splitted by comma.
 */
template <typename T>
std::vector<T> get_cmd_option_list(char** begin, char** end, const std::string& option,
                                   const std::optional<std::vector<T>>& default_value = std::nullopt) {
  const auto iter = std::find(begin, end, option);
  if (iter == end || iter + 1 == end) {
    if (static_cast<bool>(default_value)) {
      return *default_value;
    }

    Fail("Option '" + option + "' was not specified, and no default was given.");
  }

  std::vector<T> result;

  std::stringstream ss(*(iter + 1));
  std::string token;

  while (std::getline(ss, token, ',')) {
    result.push_back(str2T<T>(token));
  }

  return result;
}

/**
 * Checks whether option exists in argv.
 */
bool cmd_option_exists(char** begin, char** end, const std::string& option) {
  return std::find(begin, end, option) != end;
}

/**
 * Prints a vector python style.
 */
template <typename T>
std::string vec2str(const std::vector<T>& items) {
  if (items.empty()) {
    return "[]";
  }

  std::stringstream stream;
  stream << "[";

  for (auto idx = 0u; idx < items.size() - 1; idx++) {
    stream << items[idx] << ", ";
  }

  stream << items.back() << "]";

  return stream.str();
}

/**
 * Returns the distinct values of a segment.
 */
template <typename T>
std::unordered_set<T> get_distinct_values(const std::shared_ptr<const BaseSegment>& segment) {
  std::unordered_set<T> distinct_values;

  resolve_segment_type<T>(*segment, [&](auto& typed_segment) {
    auto iterable = create_iterable_from_segment<T>(typed_segment);
    iterable.for_each([&](const auto& value) {
      if (!value.is_null()) {
        distinct_values.insert(value.value());
      }
    });
  });

  return distinct_values;
}

/**
 * Returns the distinct values of a column.
 */
template <typename T>
std::unordered_set<T> get_distinct_values(const std::shared_ptr<const Table>& table, const ColumnID column_id) {
  std::unordered_set<T> distinct_values;

  for (const auto chunk : table->chunks()) {
    const auto segment_distinct_values = get_distinct_values<T>(chunk->get_segment(column_id));
    distinct_values.insert(segment_distinct_values.cbegin(), segment_distinct_values.cend());
  }

  return distinct_values;
}

/**
 * Returns the distinct count for a column.
 */
uint64_t get_distinct_count(const std::shared_ptr<const Table>& table, const ColumnID column_id) {
  uint64_t distinct_count;

  resolve_data_type(table->column_data_type(column_id), [&](auto type) {
    using DataTypeT = typename decltype(type)::type;
    distinct_count = get_distinct_values<DataTypeT>(table, column_id).size();
  });

  return distinct_count;
}

/**
 * Returns a hash map from column id to distinct count for all column ids occurring in filters_by_column.
 */
std::unordered_map<ColumnID, uint64_t> get_distinct_count_by_column(
    const std::shared_ptr<const Table>& table,
    const std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>>& filters_by_column) {
  std::unordered_map<ColumnID, uint64_t> distinct_count_by_column;

  for (const auto& p : filters_by_column) {
    const auto column_id = p.first;

    if (distinct_count_by_column.find(column_id) == distinct_count_by_column.end()) {
      distinct_count_by_column[column_id] = get_distinct_count(table, column_id);
    }
  }

  return distinct_count_by_column;
}

/**
 * Reads a list of filters from the specified file.
 */
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> read_filters_from_file(
    const std::string& filter_file, const std::shared_ptr<const Table>& table) {
  std::ifstream infile(filter_file);
  Assert(infile.is_open(), "Could not find file: " + filter_file + ".");

  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  std::string line;
  while (std::getline(infile, line)) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string token;

    while (std::getline(ss, token, ',')) {
      fields.push_back(token);
    }

    Assert(fields.size() == 3, "Filter file invalid in line: '" + line + "'.");

    const auto column_id = ColumnID{str2T<uint16_t>(fields[0])};
    const auto predicate_type = predicate_condition_to_string.right.at(fields[1]);

    resolve_data_type(table->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto v = AllTypeVariant{str2T<ColumnDataType>(fields[2])};
      filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
    });
  }

  return filters;
}

/**
 * Generates a list of num_filters filters on column_id with predicate_type.
 * Values are between min and max in equal steps.
 */
template <typename T>
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, const T min, const T max, const T step) {
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;
  filters.reserve((max - min) / step);

  for (auto v = min; v <= max; v += step) {
    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
  }

  return filters;
}

/**
 * Generates a list of num_filters filters on column_id with predicate_type.
 * Values are random float values. Each random value appears at most once.
 */
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, std::mt19937 gen,
    std::uniform_real_distribution<> dis, const uint32_t num_filters) {
  std::unordered_set<double> used_values;
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;
  filters.reserve(num_filters);

  for (auto i = 0u; i < num_filters; i++) {
    auto v = dis(gen);
    while (used_values.find(v) != used_values.end()) {
      v = dis(gen);
    }

    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
    used_values.insert(v);
  }

  return filters;
}

/**
 * Generates a list of num_filters filters on column_id with predicate_type.
 * Values are random int values. Each random value appears at most once.
 */
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, std::mt19937 gen,
    std::uniform_int_distribution<> dis, const uint32_t num_filters) {
  std::unordered_set<int64_t> used_values;
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;
  filters.reserve(num_filters);

  for (auto i = 0u; i < num_filters; i++) {
    auto v = dis(gen);
    while (used_values.find(v) != used_values.end()) {
      v = dis(gen);
    }

    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
    used_values.insert(v);
  }

  return filters;
}

/**
 * Generates a filter for every distinct value in a given column of the table with the given predicate.
 * Generates a maximum of num_filters.
 */
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const std::shared_ptr<const Table>& table, const ColumnID column_id, const PredicateCondition predicate_type,
    const std::optional<uint32_t> num_filters = std::nullopt) {
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  resolve_data_type(table->column_data_type(column_id), [&](auto type) {
    using T = typename decltype(type)::type;
    const auto distinct_values = get_distinct_values<T>(table, column_id);
    filters.reserve(num_filters ? *num_filters : distinct_values.size());

    auto i = 0ul;
    for (const auto& v : distinct_values) {
      if (num_filters && i >= *num_filters) {
        break;
      }

      filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
      i++;
    }
  });

  return filters;
}

/**
 * Groups filters by ColumnID and returns a hash map from ColumnID to a vector of pairs of predicate types and values.
 */
std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> get_filters_by_column(
    const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters) {
  std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> filters_by_column;

  for (const auto& filter : filters) {
    const auto column_id = std::get<0>(filter);
    const auto predicate_condition = std::get<1>(filter);
    const auto& value = std::get<2>(filter);
    filters_by_column[column_id].emplace_back(
        std::pair<PredicateCondition, AllTypeVariant>{predicate_condition, value});
  }

  return filters_by_column;
}

/**
 * Returns a list of distinct values and the number of their occurences in segment.
 */
template <typename T>
std::vector<std::pair<T, uint64_t>> calculate_value_counts(const std::shared_ptr<const Table>& table,
                                                           const ColumnID column_id) {
  std::map<T, uint64_t> value_counts;

  for (const auto chunk : table->chunks()) {
    const auto segment = chunk->get_segment(column_id);

    resolve_segment_type<T>(*segment, [&](auto& typed_segment) {
      auto iterable = create_iterable_from_segment<T>(typed_segment);
      iterable.for_each([&](const auto& value) {
        if (!value.is_null()) {
          value_counts[value.value()]++;
        }
      });
    });
  }

  std::vector<std::pair<T, uint64_t>> result(value_counts.cbegin(), value_counts.cend());
  return result;
}

/**
 * For all filters, return a map from ColumnID via predicate type via value
 * to a bool indicating whether the predicate on this column with this value is prunable.
 */
std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, uint64_t>>>
get_prunable_for_filters(
    const std::shared_ptr<const Table>& table,
    const std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>>& filters_by_column) {
  std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, uint64_t>>>
      prunable_by_filter;

  for (const auto& column_filters : filters_by_column) {
    const auto column_id = column_filters.first;
    const auto filters = column_filters.second;

    resolve_data_type(table->column_data_type(column_id), [&](auto type) {
      using T = typename decltype(type)::type;

      for (const auto& chunk : table->chunks()) {
        const auto segment = chunk->get_segment(column_id);

        const auto segment_distinct_set = get_distinct_values<T>(segment);
        std::vector<T> segment_distinct_values(segment_distinct_set.cbegin(), segment_distinct_set.cend());
        std::sort(segment_distinct_values.begin(), segment_distinct_values.end());

        for (const auto& filter : filters) {
          const auto predicate_type = std::get<0>(filter);
          const auto& value = std::get<1>(filter);
          const auto t_value = type_cast<T>(value);

          if (segment_distinct_values.empty()) {
            prunable_by_filter[column_id][predicate_type][value]++;
            continue;
          }

          switch (predicate_type) {
            case PredicateCondition::Equals: {
              const auto it = std::find(segment_distinct_values.cbegin(), segment_distinct_values.cend(), t_value);
              if (it == segment_distinct_values.cend()) {
                prunable_by_filter[column_id][predicate_type][value]++;
              }
              break;
            }
            case PredicateCondition::NotEquals: {
              if (segment_distinct_values.size() == 1 && segment_distinct_values.front() == t_value) {
                prunable_by_filter[column_id][predicate_type][value]++;
              }
              break;
            }
            case PredicateCondition::LessThan: {
              if (t_value <= segment_distinct_values.front()) {
                prunable_by_filter[column_id][predicate_type][value]++;
              }
              break;
            }
            case PredicateCondition::LessThanEquals: {
              if (t_value < segment_distinct_values.front()) {
                prunable_by_filter[column_id][predicate_type][value]++;
              }
              break;
            }
            case PredicateCondition::GreaterThanEquals: {
              if (t_value > segment_distinct_values.back()) {
                prunable_by_filter[column_id][predicate_type][value]++;
              }
              break;
            }
            case PredicateCondition::GreaterThan: {
              if (t_value >= segment_distinct_values.back()) {
                prunable_by_filter[column_id][predicate_type][value]++;
              }
              break;
            }
            case PredicateCondition::Like: {
              if constexpr (!std::is_same_v<T, std::string>) {
                Fail("LIKE predicates only work for string columns");
              } else {
                const auto regex_string = LikeMatcher::sql_like_to_regex(t_value);
                const auto regex = std::regex{regex_string};

                auto prunable = true;
                for (const auto& value : segment_distinct_values) {
                  if (std::regex_match(value, regex)) {
                    prunable = false;
                    break;
                  }
                }

                if (prunable) {
                  prunable_by_filter[column_id][predicate_type][value]++;
                }
              }
              break;
            }
            case PredicateCondition::NotLike: {
              if constexpr (!std::is_same_v<T, std::string>) {
                Fail("NOT LIKE predicates only work for string columns");
              } else {
                const auto regex_string = LikeMatcher::sql_like_to_regex(t_value);
                const auto regex = std::regex{regex_string};

                auto prunable = true;
                for (const auto& value : segment_distinct_values) {
                  if (!std::regex_match(value, regex)) {
                    prunable = false;
                    break;
                  }
                }

                if (prunable) {
                  prunable_by_filter[column_id][predicate_type][value]++;
                }
              }
              break;
            }
            default:
              Fail("Predicate type not supported.");
          }
        }
      }
    });
  }

  return prunable_by_filter;
}

/**
 * For all filters, return a map from ColumnID via predicate type via value
 * to the number of rows matching the predicate on this column with this value.
 */
std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, uint64_t>>>
get_row_count_for_filters(
    const std::shared_ptr<const Table>& table,
    const std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>>& filters_by_column) {
  std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, uint64_t>>>
      row_count_by_filter;

  const auto total_count = table->row_count();

  for (const auto& column_filters : filters_by_column) {
    const auto column_id = column_filters.first;
    const auto filters = column_filters.second;

    resolve_data_type(table->column_data_type(column_id), [&](auto type) {
      using T = typename decltype(type)::type;

      const auto value_counts = calculate_value_counts<T>(table, column_id);

      for (const auto& filter : filters) {
        const auto predicate_type = std::get<0>(filter);
        const auto& value = std::get<1>(filter);
        const auto t_value = type_cast<T>(value);

        switch (predicate_type) {
          case PredicateCondition::Equals: {
            const auto it = std::find_if(value_counts.cbegin(), value_counts.cend(),
                                         [&](const std::pair<T, uint64_t>& p) { return p.first == t_value; });
            if (it != value_counts.cend()) {
              row_count_by_filter[column_id][predicate_type][value] = (*it).second;
            }
            break;
          }
          case PredicateCondition::NotEquals: {
            const auto it = std::find_if(value_counts.cbegin(), value_counts.cend(),
                                         [&](const std::pair<T, uint64_t>& p) { return p.first == t_value; });
            uint64_t count = total_count;
            if (it != value_counts.cend()) {
              count -= (*it).second;
            }
            row_count_by_filter[column_id][predicate_type][value] = count;
            break;
          }
          case PredicateCondition::LessThan: {
            const auto it =
                std::lower_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const std::pair<T, uint64_t>& lhs, const T& rhs) { return lhs.first < rhs; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(value_counts.cbegin(), it, uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::LessThanEquals: {
            const auto it =
                std::upper_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const T& lhs, const std::pair<T, uint64_t>& rhs) { return lhs < rhs.first; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(value_counts.cbegin(), it, uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::GreaterThanEquals: {
            const auto it =
                std::lower_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const std::pair<T, uint64_t>& lhs, const T& rhs) { return lhs.first < rhs; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(it, value_counts.cend(), uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::GreaterThan: {
            const auto it =
                std::upper_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const T& lhs, const std::pair<T, uint64_t>& rhs) { return lhs < rhs.first; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(it, value_counts.cend(), uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::Like: {
            if constexpr (!std::is_same_v<T, std::string>) {
              Fail("LIKE predicates only work for string columns");
            } else {
              const auto regex_string = LikeMatcher::sql_like_to_regex(t_value);
              const auto regex = std::regex{regex_string};

              uint64_t sum = 0;
              for (const auto& p : value_counts) {
                if (std::regex_match(p.first, regex)) {
                  sum += p.second;
                }
              }

              row_count_by_filter[column_id][predicate_type][value] = sum;
            }
            break;
          }
          case PredicateCondition::NotLike: {
            if constexpr (!std::is_same_v<T, std::string>) {
              Fail("NOT LIKE predicates only work for string columns");
            } else {
              const auto regex_string = LikeMatcher::sql_like_to_regex(t_value);
              const auto regex = std::regex{regex_string};

              uint64_t sum = 0;
              for (const auto& p : value_counts) {
                if (!std::regex_match(p.first, regex)) {
                  sum += p.second;
                }
              }

              row_count_by_filter[column_id][predicate_type][value] = sum;
            }
            break;
          }
          default:
            Fail("Predicate type not supported.");
        }
      }
    });
  }

  return row_count_by_filter;
}

template <typename T>
std::vector<std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>, std::shared_ptr<EqualHeightHistogram<T>>,
                       std::shared_ptr<EqualWidthHistogram<T>>>>
create_histograms_for_column(const std::shared_ptr<const Table> table, const ColumnID column_id, BinID num_bins) {
  std::vector<std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>, std::shared_ptr<EqualHeightHistogram<T>>,
                         std::shared_ptr<EqualWidthHistogram<T>>>>
      histograms;

  for (const auto chunk : table->chunks()) {
    const auto equal_distinct_count_hist =
        EqualDistinctCountHistogram<T>::from_segment(chunk->get_segment(column_id), num_bins);
    const auto equal_height_hist = EqualHeightHistogram<T>::from_segment(chunk->get_segment(column_id), num_bins);
    const auto equal_width_hist = EqualWidthHistogram<T>::from_segment(chunk->get_segment(column_id), num_bins);

    histograms.emplace_back(equal_distinct_count_hist, equal_height_hist, equal_width_hist);
  }

  return histograms;
}

template <typename T>
std::shared_ptr<CountingQuotientFilter<T>> build_cqf(const std::shared_ptr<const BaseSegment>& segment,
                                                     const uint8_t remainder = 8u) {
  const auto distinct_count = get_distinct_values<T>(segment).size();
  const auto quotient_size = static_cast<size_t>(std::ceil(
      std::log2(distinct_count * (1 + std::log2(static_cast<double>(segment->size()) / distinct_count) / remainder))));
  const auto cqf = std::make_shared<CountingQuotientFilter<T>>(quotient_size, remainder);
  cqf->populate(segment);
  return cqf;
}

template <typename T>
std::vector<std::shared_ptr<CountingQuotientFilter<T>>> create_cqfs_for_column(const std::shared_ptr<const Table> table,
                                                                               const ColumnID column_id) {
  std::vector<std::shared_ptr<CountingQuotientFilter<T>>> cqfs;

  for (const auto chunk : table->chunks()) {
    cqfs.emplace_back(build_cqf<T>(chunk->get_segment(column_id)));
  }

  return cqfs;
}

template <typename T>
std::shared_ptr<MinMaxFilter<T>> build_minmax(const std::shared_ptr<const BaseSegment>& segment) {
  const auto distinct_values = get_distinct_values<T>(segment);
  const auto [min, max] = std::minmax_element(distinct_values.cbegin(), distinct_values.cend());
  return std::make_shared<MinMaxFilter<T>>(*min, *max);
}

template <typename T>
std::vector<std::shared_ptr<MinMaxFilter<T>>> create_minmax_for_column(const std::shared_ptr<const Table> table,
                                                                       const ColumnID column_id) {
  std::vector<std::shared_ptr<MinMaxFilter<T>>> filter;

  for (const auto chunk : table->chunks()) {
    filter.emplace_back(build_minmax<T>(chunk->get_segment(column_id)));
  }

  return filter;
}

template <typename T>
std::unique_ptr<RangeFilter<T>> build_rangefilter(const std::shared_ptr<const BaseSegment>& segment,
                                                  const size_t bin_count) {
  const auto distinct_set = get_distinct_values<T>(segment);
  auto dictionary = pmr_vector<T>{distinct_set.cbegin(), distinct_set.cend()};
  std::sort(dictionary.begin(), dictionary.end());
  return RangeFilter<T>::build_filter(dictionary, bin_count);
}

template <typename T>
std::vector<std::unique_ptr<RangeFilter<T>>> create_rangefilter_for_column(const std::shared_ptr<const Table> table,
                                                                           const ColumnID column_id,
                                                                           const size_t bin_count) {
  std::vector<std::unique_ptr<RangeFilter<T>>> filter;

  for (const auto chunk : table->chunks()) {
    filter.emplace_back(build_rangefilter<T>(chunk->get_segment(column_id), bin_count));
  }

  return filter;
}

template <typename T>
void print_bins_to_csv(
    const std::vector<std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                 std::shared_ptr<EqualHeightHistogram<T>>, std::shared_ptr<EqualWidthHistogram<T>>>>&
        histograms,
    const std::string& column_name, const uint64_t num_bins, std::ofstream& bin_log) {
  for (auto idx = ChunkID{0}; idx < histograms.size(); ++idx) {
    const auto equal_distinct_count_hist = std::get<0>(histograms[idx]);

    if (!equal_distinct_count_hist) {
      continue;
    }

    const auto equal_height_hist = std::get<1>(histograms[idx]);
    const auto equal_width_hist = std::get<2>(histograms[idx]);

    bin_log << equal_distinct_count_hist->bins_to_csv(false, column_name, num_bins, idx);
    bin_log << equal_height_hist->bins_to_csv(false, column_name, num_bins, idx);
    bin_log << equal_width_hist->bins_to_csv(false, column_name, num_bins, idx);
    bin_log.flush();
  }
}

template <typename T>
void print_memory_to_csv(
    const std::vector<std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                 std::shared_ptr<EqualHeightHistogram<T>>, std::shared_ptr<EqualWidthHistogram<T>>>>&
        histograms,
    const std::vector<std::shared_ptr<CountingQuotientFilter<T>>>& cqfs, const std::string& column_name,
    const uint64_t num_bins, std::ofstream& memory_log) {
  for (auto idx = ChunkID{0}; idx < histograms.size(); ++idx) {
    const auto equal_distinct_count_hist = std::get<0>(histograms[idx]);

    if (!equal_distinct_count_hist) {
      continue;
    }

    const auto equal_height_hist = std::get<1>(histograms[idx]);
    const auto equal_width_hist = std::get<2>(histograms[idx]);

    memory_log << column_name << ",";
    memory_log << num_bins << ",";
    memory_log << idx << ",";

    memory_log << cqfs[idx]->memory_consumption() << ",";

    memory_log << equal_height_hist->estimated_memory_footprint() << ",";
    memory_log << equal_distinct_count_hist->estimated_memory_footprint() << ",";
    memory_log << equal_width_hist->estimated_memory_footprint() << "\n";
    memory_log.flush();
  }
}

template <typename T>
void print_memory_to_csv(
    const std::vector<std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                 std::shared_ptr<EqualHeightHistogram<T>>, std::shared_ptr<EqualWidthHistogram<T>>>>&
        histograms,
    const std::vector<std::shared_ptr<MinMaxFilter<T>>>& filter, const std::string& column_name,
    const uint64_t num_bins, std::ofstream& memory_log) {
  for (auto idx = ChunkID{0}; idx < histograms.size(); ++idx) {
    const auto equal_distinct_count_hist = std::get<0>(histograms[idx]);

    if (!equal_distinct_count_hist) {
      continue;
    }

    const auto equal_height_hist = std::get<1>(histograms[idx]);
    const auto equal_width_hist = std::get<2>(histograms[idx]);

    memory_log << column_name << ",";
    memory_log << num_bins << ",";
    memory_log << idx << ",";

    memory_log << filter[idx]->estimated_memory_footprint() << ",";

    memory_log << equal_height_hist->estimated_memory_footprint() << ",";
    memory_log << equal_distinct_count_hist->estimated_memory_footprint() << ",";
    memory_log << equal_width_hist->estimated_memory_footprint() << "\n";
    memory_log.flush();
  }
}

template <typename T>
void print_memory_to_csv(
    const std::vector<std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                 std::shared_ptr<EqualHeightHistogram<T>>, std::shared_ptr<EqualWidthHistogram<T>>>>&
        histograms,
    const std::vector<std::unique_ptr<RangeFilter<T>>>& filter, const std::string& column_name, const uint64_t num_bins,
    std::ofstream& memory_log) {
  for (auto idx = ChunkID{0}; idx < histograms.size(); ++idx) {
    const auto equal_distinct_count_hist = std::get<0>(histograms[idx]);

    if (!equal_distinct_count_hist) {
      continue;
    }

    const auto equal_height_hist = std::get<1>(histograms[idx]);
    const auto equal_width_hist = std::get<2>(histograms[idx]);

    memory_log << column_name << ",";
    memory_log << num_bins << ",";
    memory_log << idx << ",";

    memory_log << filter[idx]->estimated_memory_footprint() << ",";

    memory_log << equal_height_hist->estimated_memory_footprint() << ",";
    memory_log << equal_distinct_count_hist->estimated_memory_footprint() << ",";
    memory_log << equal_width_hist->estimated_memory_footprint() << "\n";
    memory_log.flush();
  }
}

void run_pruning(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                 const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                 std::ofstream& result_log, std::ofstream& bin_log, std::ofstream& memory_log) {
  log("Running pruning...");

  const auto chunk_size = table->max_chunk_size();
  const auto filters_by_column = get_filters_by_column(filters);
  const auto prunable_by_filter = get_prunable_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);
        print_bins_to_csv<T>(histograms, column_name, num_bins, bin_log);

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;
          const auto value = pair.second;

          auto prunable_count = uint64_t{0};
          const auto prunable_column_it = prunable_by_filter.find(column_id);
          if (prunable_column_it != prunable_by_filter.end()) {
            const auto prunable_predicate_it = prunable_column_it->second.find(predicate_condition);
            if (prunable_predicate_it != prunable_column_it->second.end()) {
              const auto prunable_value_it = prunable_predicate_it->second.find(value);
              if (prunable_value_it != prunable_predicate_it->second.end()) {
                prunable_count = prunable_value_it->second;
              }
            }
          }

          const auto equal_distinct_count_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<0>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          const auto equal_height_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<1>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          const auto equal_width_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<2>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          result_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                     << std::to_string(chunk_size) << "," << std::to_string(num_bins) << "," << column_name << ","
                     << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                     << std::to_string(prunable_count) << "," << std::to_string(equal_height_hist_prunable) << ","
                     << std::to_string(equal_distinct_count_hist_prunable) << ","
                     << std::to_string(equal_width_hist_prunable) << "\n";
          result_log.flush();
        }
      });
    }
  }
}

void run_estimation(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                    const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                    std::ofstream& result_log, std::ofstream& bin_log, std::ofstream& memory_log) {
  log("Running estimation...");

  const auto chunk_size = table->max_chunk_size();
  const auto filters_by_column = get_filters_by_column(filters);
  const auto row_count_by_filter = get_row_count_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);
        print_bins_to_csv<T>(histograms, column_name, num_bins, bin_log);

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;
          const auto value = pair.second;

          const auto actual_count = row_count_by_filter.at(column_id).at(predicate_condition).at(value);
          const auto equal_distinct_count_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<0>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          const auto equal_height_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<1>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          const auto equal_width_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<2>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          result_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                     << std::to_string(chunk_size) << "," << std::to_string(num_bins) << "," << column_name << ","
                     << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                     << std::to_string(actual_count) << "," << std::to_string(equal_height_hist_count) << ","
                     << std::to_string(equal_distinct_count_hist_count) << "," << std::to_string(equal_width_hist_count)
                     << "\n";
          result_log.flush();
        }
      });
    }
  }
}

void run_estimation_cqf(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                        const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                        std::ofstream& result_log, std::ofstream& bin_log, std::ofstream& memory_log) {
  log("Running CQF estimation...");

  const auto chunk_size = table->max_chunk_size();
  const auto filters_by_column = get_filters_by_column(filters);
  const auto row_count_by_filter = get_row_count_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto cqfs = create_cqfs_for_column<T>(table, column_id);
        const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);
        print_bins_to_csv<T>(histograms, column_name, num_bins, bin_log);
        print_memory_to_csv<T>(histograms, cqfs, column_name, num_bins, memory_log);

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;

          if (predicate_condition != PredicateCondition::Equals) {
            log("Skipping filter because CQFs can only handle equality predicates...");
            continue;
          }

          const auto value = pair.second;

          const auto actual_count = row_count_by_filter.at(column_id).at(predicate_condition).at(value);
          const auto cqf_count = std::accumulate(
              cqfs.cbegin(), cqfs.cend(), float{0},
              [&](float a, const std::shared_ptr<CountingQuotientFilter<T>>& b) { return a + b->count(value); });
          const auto equal_distinct_count_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<0>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          const auto equal_height_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<1>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          const auto equal_width_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<2>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          result_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                     << std::to_string(chunk_size) << "," << std::to_string(num_bins) << "," << column_name << ","
                     << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                     << std::to_string(actual_count) << "," << std::to_string(cqf_count) << ","
                     << std::to_string(equal_height_hist_count) << ","
                     << std::to_string(equal_distinct_count_hist_count) << "," << std::to_string(equal_width_hist_count)
                     << "\n";
          result_log.flush();
        }
      });
    }
  }
}

void run_pruning_cqf(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                     const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                     std::ofstream& result_log, std::ofstream& bin_log, std::ofstream& memory_log) {
  log("Running CQF pruning...");

  const auto chunk_size = table->max_chunk_size();
  const auto filters_by_column = get_filters_by_column(filters);
  const auto prunable_by_filter = get_prunable_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto cqfs = create_cqfs_for_column<T>(table, column_id);
        const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);
        print_bins_to_csv<T>(histograms, column_name, num_bins, bin_log);
        print_memory_to_csv<T>(histograms, cqfs, column_name, num_bins, memory_log);

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;

          if (predicate_condition != PredicateCondition::Equals) {
            log("Skipping filter because CQFs can only handle equality predicates...");
            continue;
          }

          const auto value = pair.second;

          auto prunable_count = uint64_t{0};
          const auto prunable_column_it = prunable_by_filter.find(column_id);
          if (prunable_column_it != prunable_by_filter.end()) {
            const auto prunable_predicate_it = prunable_column_it->second.find(predicate_condition);
            if (prunable_predicate_it != prunable_column_it->second.end()) {
              const auto prunable_value_it = prunable_predicate_it->second.find(value);
              if (prunable_value_it != prunable_predicate_it->second.end()) {
                prunable_count = prunable_value_it->second;
              }
            }
          }

          const auto cqf_prunable =
              std::accumulate(cqfs.cbegin(), cqfs.cend(), uint64_t{0},
                              [&](uint64_t a, const std::shared_ptr<CountingQuotientFilter<T>>& b) {
                                return a + b->can_prune(predicate_condition, value);
                              });

          const auto equal_distinct_count_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<0>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          const auto equal_height_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<1>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          const auto equal_width_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<2>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          result_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                     << std::to_string(chunk_size) << "," << std::to_string(num_bins) << "," << column_name << ","
                     << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                     << std::to_string(prunable_count) << "," << std::to_string(cqf_prunable) << ","
                     << std::to_string(equal_height_hist_prunable) << ","
                     << std::to_string(equal_distinct_count_hist_prunable) << ","
                     << std::to_string(equal_width_hist_prunable) << "\n";
          result_log.flush();
        }
      });
    }
  }
}

void run_estimation_minmaxdistinct(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                                   const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                                   std::ofstream& result_log, std::ofstream& bin_log, std::ofstream& memory_log) {
  log("Running MinMaxDistinct estimation...");

  const auto chunk_size = table->max_chunk_size();
  const auto filters_by_column = get_filters_by_column(filters);
  const auto row_count_by_filter = get_row_count_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto minmax = generate_column_statistics<T>(*table, column_id);
        const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);
        print_bins_to_csv<T>(histograms, column_name, num_bins, bin_log);

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;
          const auto value = pair.second;

          const auto actual_count = row_count_by_filter.at(column_id).at(predicate_condition).at(value);
          const auto minmax_count =
              minmax->estimate_predicate_with_value(predicate_condition, value).selectivity * total_count;
          const auto equal_distinct_count_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<0>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          const auto equal_height_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<1>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          const auto equal_width_hist_count =
              std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                              [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                            std::shared_ptr<EqualHeightHistogram<T>>,
                                                            std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<2>(b);
                                // hist is a nullptr if segment only contains null values.
                                if (!hist) {
                                  return a;
                                }
                                return a + hist->estimate_cardinality(predicate_condition, value);
                              });

          result_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                     << std::to_string(chunk_size) << "," << std::to_string(num_bins) << "," << column_name << ","
                     << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                     << std::to_string(actual_count) << "," << std::to_string(minmax_count) << ","
                     << std::to_string(equal_height_hist_count) << ","
                     << std::to_string(equal_distinct_count_hist_count) << "," << std::to_string(equal_width_hist_count)
                     << "\n";
          result_log.flush();
        }
      });
    }
  }
}

void run_pruning_minmax(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                        const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                        std::ofstream& result_log, std::ofstream& bin_log, std::ofstream& memory_log) {
  log("Running MinMax pruning...");

  const auto chunk_size = table->max_chunk_size();
  const auto filters_by_column = get_filters_by_column(filters);
  const auto prunable_by_filter = get_prunable_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto filter = create_minmax_for_column<T>(table, column_id);
        const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);
        print_bins_to_csv<T>(histograms, column_name, num_bins, bin_log);
        print_memory_to_csv<T>(histograms, filter, column_name, num_bins, memory_log);

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;
          const auto value = pair.second;

          auto prunable_count = uint64_t{0};
          const auto prunable_column_it = prunable_by_filter.find(column_id);
          if (prunable_column_it != prunable_by_filter.end()) {
            const auto prunable_predicate_it = prunable_column_it->second.find(predicate_condition);
            if (prunable_predicate_it != prunable_column_it->second.end()) {
              const auto prunable_value_it = prunable_predicate_it->second.find(value);
              if (prunable_value_it != prunable_predicate_it->second.end()) {
                prunable_count = prunable_value_it->second;
              }
            }
          }

          const auto minmax_prunable = std::accumulate(filter.cbegin(), filter.cend(), uint64_t{0},
                                                       [&](uint64_t a, const std::shared_ptr<MinMaxFilter<T>>& b) {
                                                         return a + b->can_prune(predicate_condition, value);
                                                       });
          const auto equal_distinct_count_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<0>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          const auto equal_height_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<1>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          const auto equal_width_hist_prunable =
              std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                              [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                               std::shared_ptr<EqualHeightHistogram<T>>,
                                                               std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                const auto hist = std::get<2>(b);
                                // hist is a nullptr if the segment has only null values.
                                if (!hist) {
                                  return a + 1;
                                }
                                return a + hist->can_prune(predicate_condition, value);
                              });

          result_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                     << std::to_string(chunk_size) << "," << std::to_string(num_bins) << "," << column_name << ","
                     << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                     << std::to_string(prunable_count) << "," << std::to_string(minmax_prunable) << ","
                     << std::to_string(equal_height_hist_prunable) << ","
                     << std::to_string(equal_distinct_count_hist_prunable) << ","
                     << std::to_string(equal_width_hist_prunable) << "\n";
          result_log.flush();
        }
      });
    }
  }
}

void run_pruning_range(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                       const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                       std::ofstream& result_log, std::ofstream& bin_log, std::ofstream& memory_log) {
  log("Running Range pruning...");

  const auto chunk_size = table->max_chunk_size();
  const auto filters_by_column = get_filters_by_column(filters);
  const auto prunable_by_filter = get_prunable_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        if constexpr (std::is_arithmetic_v<T>) {
          const auto filter = create_rangefilter_for_column<T>(table, column_id, num_bins);
          const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);
          print_bins_to_csv<T>(histograms, column_name, num_bins, bin_log);
          print_memory_to_csv<T>(histograms, filter, column_name, num_bins, memory_log);

          for (const auto& pair : it.second) {
            const auto predicate_condition = pair.first;
            const auto value = pair.second;

            auto prunable_count = uint64_t{0};
            const auto prunable_column_it = prunable_by_filter.find(column_id);
            if (prunable_column_it != prunable_by_filter.end()) {
              const auto prunable_predicate_it = prunable_column_it->second.find(predicate_condition);
              if (prunable_predicate_it != prunable_column_it->second.end()) {
                const auto prunable_value_it = prunable_predicate_it->second.find(value);
                if (prunable_value_it != prunable_predicate_it->second.end()) {
                  prunable_count = prunable_value_it->second;
                }
              }
            }

            const auto range_prunable = std::accumulate(filter.cbegin(), filter.cend(), uint64_t{0},
                                                        [&](uint64_t a, const std::unique_ptr<RangeFilter<T>>& b) {
                                                          return a + b->can_prune(predicate_condition, value);
                                                        });
            const auto equal_distinct_count_hist_prunable =
                std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                                [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                                 std::shared_ptr<EqualHeightHistogram<T>>,
                                                                 std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                  const auto hist = std::get<0>(b);
                                  // hist is a nullptr if the segment has only null values.
                                  if (!hist) {
                                    return a + 1;
                                  }
                                  return a + hist->can_prune(predicate_condition, value);
                                });

            const auto equal_height_hist_prunable =
                std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                                [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                                 std::shared_ptr<EqualHeightHistogram<T>>,
                                                                 std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                  const auto hist = std::get<1>(b);
                                  // hist is a nullptr if the segment has only null values.
                                  if (!hist) {
                                    return a + 1;
                                  }
                                  return a + hist->can_prune(predicate_condition, value);
                                });

            const auto equal_width_hist_prunable =
                std::accumulate(histograms.cbegin(), histograms.cend(), uint64_t{0},
                                [&](uint64_t a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                                 std::shared_ptr<EqualHeightHistogram<T>>,
                                                                 std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                                  const auto hist = std::get<2>(b);
                                  // hist is a nullptr if the segment has only null values.
                                  if (!hist) {
                                    return a + 1;
                                  }
                                  return a + hist->can_prune(predicate_condition, value);
                                });

            result_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                       << std::to_string(chunk_size) << "," << std::to_string(num_bins) << "," << column_name << ","
                       << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                       << std::to_string(prunable_count) << "," << std::to_string(range_prunable) << ","
                       << std::to_string(equal_height_hist_prunable) << ","
                       << std::to_string(equal_distinct_count_hist_prunable) << ","
                       << std::to_string(equal_width_hist_prunable) << "\n";
            result_log.flush();
          }
        } else {
          log("Cannot run range pruning on string column.");
        }
      });
    }
  }
}

void time_estimation(const std::shared_ptr<const Table> table, const std::vector<uint64_t> num_bins_list,
                     const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
                     std::ofstream& result_log) {
  log("Timing estimation...");

  const auto filters_by_column = get_filters_by_column(filters);

  for (auto num_bins : num_bins_list) {
    log("  " + std::to_string(num_bins) + " bins...");

    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto column_name = table->column_name(column_id);

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto minmax = generate_column_statistics<T>(*table, column_id);
        const auto cqfs = create_cqfs_for_column<T>(table, column_id);
        const auto histograms = create_histograms_for_column<T>(table, column_id, num_bins);

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;

          if (predicate_condition != PredicateCondition::Equals) {
            log("Skipping filter because CQFs can only handle equality predicates...");
            continue;
          }

          const auto value = pair.second;

          const auto iteration_count = 1000u;

          const auto minmax_start = std::chrono::high_resolution_clock::now();
          for (auto i = 0u; i < iteration_count; i++) {
            minmax->estimate_predicate_with_value(predicate_condition, value);
          }
          const auto minmax_end = std::chrono::high_resolution_clock::now();
          const auto minmax_time =
              std::chrono::duration_cast<std::chrono::microseconds>(minmax_end - minmax_start).count();

          // float cqf_count;
          const auto cqf_start = std::chrono::high_resolution_clock::now();
          for (auto i = 0u; i < iteration_count; i++) {
            // cqf_count =
            std::accumulate(
                cqfs.cbegin(), cqfs.cend(), float{0},
                [&](float a, const std::shared_ptr<CountingQuotientFilter<T>>& b) { return a + b->count(value); });
          }
          const auto cqf_end = std::chrono::high_resolution_clock::now();
          const auto cqf_time = std::chrono::duration_cast<std::chrono::microseconds>(cqf_end - cqf_start).count();

          // float equal_distinct_count_hist_count;
          const auto distinct_start = std::chrono::high_resolution_clock::now();
          for (auto i = 0u; i < iteration_count; i++) {
            // equal_distinct_count_hist_count =
            std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                            [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                          std::shared_ptr<EqualHeightHistogram<T>>,
                                                          std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                              const auto hist = std::get<0>(b);
                              // hist is a nullptr if segment only contains null values.
                              if (!hist) {
                                return a;
                              }
                              return a + hist->estimate_cardinality(predicate_condition, value);
                            });
          }
          const auto distinct_end = std::chrono::high_resolution_clock::now();
          const auto distinct_time =
              std::chrono::duration_cast<std::chrono::microseconds>(distinct_end - distinct_start).count();

          // float equal_height_hist_count;
          const auto height_start = std::chrono::high_resolution_clock::now();
          for (auto i = 0u; i < iteration_count; i++) {
            // equal_height_hist_count =
            std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                            [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                          std::shared_ptr<EqualHeightHistogram<T>>,
                                                          std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                              const auto hist = std::get<1>(b);
                              // hist is a nullptr if segment only contains null values.
                              if (!hist) {
                                return a;
                              }
                              return a + hist->estimate_cardinality(predicate_condition, value);
                            });
          }
          const auto height_end = std::chrono::high_resolution_clock::now();
          const auto height_time =
              std::chrono::duration_cast<std::chrono::microseconds>(height_end - height_start).count();

          // float equal_width_hist_count;
          const auto width_start = std::chrono::high_resolution_clock::now();
          for (auto i = 0u; i < iteration_count; i++) {
            // equal_width_hist_count =
            std::accumulate(histograms.cbegin(), histograms.cend(), float{0},
                            [&](float a, const std::tuple<std::shared_ptr<EqualDistinctCountHistogram<T>>,
                                                          std::shared_ptr<EqualHeightHistogram<T>>,
                                                          std::shared_ptr<EqualWidthHistogram<T>>>& b) {
                              const auto hist = std::get<2>(b);
                              // hist is a nullptr if segment only contains null values.
                              if (!hist) {
                                return a;
                              }
                              return a + hist->estimate_cardinality(predicate_condition, value);
                            });
          }
          const auto width_end = std::chrono::high_resolution_clock::now();
          const auto width_time =
              std::chrono::duration_cast<std::chrono::microseconds>(width_end - width_start).count();

          result_log << std::to_string(num_bins) << "," << std::to_string(iteration_count) << "," << column_name << ","
                     << predicate_condition_to_string.left.at(predicate_condition) << ",";

          auto t_value = type_cast<T>(value);

          if constexpr (std::is_same_v<T, std::string>) {
            const auto patterns = std::array<std::pair<const char*, const char*>, 2u>{{{"\\", "\\\\"}, {"\"", "\\\""}}};

            for (const auto& pair : patterns) {
              boost::replace_all(t_value, pair.first, pair.second);
            }

            result_log << "\"" << t_value << "\",";
          } else {
            result_log << std::to_string(t_value) << ",";
          }

          result_log << std::to_string(minmax_time) << "," << std::to_string(cqf_time) << ","
                     << std::to_string(height_time) << "," << std::to_string(distinct_time) << ","
                     << std::to_string(width_time) << "\n";
          result_log.flush();
        }
      });
    }
  }
}

template <typename T>
std::pair<HistogramType, BinID> histogram_heuristic(T& min, T& max, HistogramCountType total_count,
                                                    HistogramCountType distinct_count) {
  constexpr auto MAX_BINS_ACCURATE_HISTOGRAM = 100u;
  constexpr auto MAX_BINS_HISTOGRAM = 1000u;

  /**
   * If we have few distinct values, use an accurate representation of the data with an EqualDistinctCountHistogram.
   */
  if (distinct_count <= MAX_BINS_ACCURATE_HISTOGRAM) {
    // This should return an AccurateHistogram, once it is implemented.
    return std::make_pair(HistogramType::EqualDistinctCount, distinct_count);
  }

  /**
   * If 80% or more of the total values are distinct (i.e., the column is almost unique), use a histogram with one bin.
   */
  if (distinct_count >= total_count * 8 / 10) {
    // This should return a SingleBinHistogram.
    std::make_pair(HistogramType::EqualWidth, 1u);
  }

  // Aim for ten values per bin, but cap at MAX_BINS_HISTOGRAM.
  const auto bin_count = std::min(distinct_count / 10, MAX_BINS_HISTOGRAM);
  // Use EqualDistinctCountHistogram, except if there is a good reason not to.
  auto histogram_type = HistogramType::EqualDistinctCount;

  /**
   * If 80% or more of all possible distinct values between min and max are actually present,
   * use an EqualWidthHistogram, because there will be few gaps anyways.
   */
  if constexpr (std::is_integral_v<T>) {
    if ((max - min + 1) * 8 / 10 <= distinct_count) {
      histogram_type = HistogramType::EqualWidth;
    }
  }

  /**
   * If the string values are expected to be rather long, use an EqualWidthHistogram,
   * because its memory consumption does not scale with the length of the string.
   * If the value range of strings has a common prefix longer than 8,
   * we do not use an EqualWidthHistogram.
   * That's because its bin boundaries are based on substrings, and if the substrings all share a large common prefix,
   * we cannot create many bins.
   */
  if constexpr (std::is_same_v<T, std::string>) {
    if (max.length() > 10 && common_prefix_length(min, max) <= 8) {
      histogram_type = HistogramType::EqualWidth;
    }
  }

  return std::make_pair(histogram_type, bin_count);
}

int main(int argc, char** argv) {
  const auto argv_end = argv + argc;
  const auto table_path = get_cmd_option<std::string>(argv, argv_end, "--table-path");
  const auto filter_mode = get_cmd_option<std::string>(argv, argv_end, "--filter-mode");
  const auto num_bins_list = get_cmd_option_list<uint64_t>(argv, argv_end, "--num-bins");
  const auto chunk_sizes =
      get_cmd_option_list<uint32_t>(argv, argv_end, "--chunk-sizes", std::vector<uint32_t>{Chunk::MAX_SIZE});
  const auto output_path = get_cmd_option<std::string>(argv, argv_end, "--output-path", "../results/");

  std::shared_ptr<Table> empty_table;
  if (cmd_option_exists(argv, argv_end, "--meta-path")) {
    CsvParser parser;
    empty_table = parser.create_table_from_meta_file(get_cmd_option<std::string>(argv, argv_end, "--meta-path"));
  } else if (cmd_option_exists(argv, argv_end, "--binary")) {
    empty_table = ImportBinary{table_path}.create_table_from_header();
    Assert(chunk_sizes.size() == 1u, "Cannot vary chunk size for binary files.");
    Assert(empty_table->max_chunk_size() == chunk_sizes[0],
           "Chunk size of binary table does not match specified chunk size.");
  } else {
    empty_table = create_table_from_header(table_path);
  }

  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  if (filter_mode == "random-on-column") {
    std::random_device rd;
    std::mt19937 gen(rd());

    const auto column_id = ColumnID{get_cmd_option<uint16_t>(argv, argv_end, "--column-id")};
    const auto predicate_type =
        predicate_condition_to_string.right.at(get_cmd_option<std::string>(argv, argv_end, "--predicate-type"));
    const auto num_filters = get_cmd_option<uint32_t>(argv, argv_end, "--num-filters");
    const auto data_type = empty_table->column_data_type(column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto min = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-min");
      const auto max = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-max");
      Assert(min < max, "Min has to be smaller than max.");

      if constexpr (std::is_floating_point_v<ColumnDataType>) {
        std::uniform_real_distribution<> dis(min, max);
        filters = generate_filters(column_id, predicate_type, gen, dis, num_filters);
      } else if constexpr (std::is_integral_v<ColumnDataType>) {
        Assert(static_cast<uint64_t>(max - min) + 1 >= num_filters,
               "Cannot generate " + std::to_string(num_filters) + " unique random values between " +
                   std::to_string(min) + " and " + std::to_string(max) + ".");
        std::uniform_int_distribution<> dis(min, max);
        filters = generate_filters(column_id, predicate_type, gen, dis, num_filters);
      } else {
        Fail("Data type not supported to generate random values.");
      }
    });
  } else if (filter_mode == "step-on-column") {
    const auto column_id = ColumnID{get_cmd_option<uint16_t>(argv, argv_end, "--column-id")};
    const auto predicate_type =
        predicate_condition_to_string.right.at(get_cmd_option<std::string>(argv, argv_end, "--predicate-type"));
    const auto data_type = empty_table->column_data_type(column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto min = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-min");
      const auto max = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-max");
      const auto step = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-step");

      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        filters = generate_filters(column_id, predicate_type, min, max, step);
      } else {
        Fail("Data type not supported to generate values in steps.");
      }
    });
  } else if (filter_mode == "distinct-on-column") {
    const auto column_id = ColumnID{get_cmd_option<uint16_t>(argv, argv_end, "--column-id")};
    const auto predicate_type =
        predicate_condition_to_string.right.at(get_cmd_option<std::string>(argv, argv_end, "--predicate-type"));

    std::optional<uint32_t> num_filters;
    if (cmd_option_exists(argv, argv_end, "--num-filters")) {
      num_filters = get_cmd_option<uint32_t>(argv, argv_end, "--num-filters");
    }

    std::shared_ptr<const Table> table;
    if (cmd_option_exists(argv, argv_end, "--meta-path")) {
      const auto csv_meta = process_csv_meta_file(get_cmd_option<std::string>(argv, argv_end, "--meta-path"));
      auto importer = std::make_shared<ImportCsv>(table_path, csv_meta);
      importer->execute();
      table = importer->get_output();
    } else if (cmd_option_exists(argv, argv_end, "--binary")) {
      auto importer = std::make_shared<ImportBinary>(table_path);
      importer->execute();
      table = importer->get_output();
    } else {
      table = load_table(table_path, Chunk::MAX_SIZE);
    }

    filters = generate_filters(table, column_id, predicate_type, num_filters);
  } else if (filter_mode == "from_file") {
    const auto filter_file = get_cmd_option<std::string>(argv, argv_end, "--filter-file");
    filters = read_filters_from_file(filter_file, empty_table);
  } else {
    Fail("Mode '" + filter_mode + "' not supported.");
  }

  std::ofstream result_log = std::ofstream(output_path + "/results.log", std::ios_base::out | std::ios_base::trunc);
  std::ofstream memory_log = std::ofstream(output_path + "/memory.log", std::ios_base::out | std::ios_base::trunc);

  if (cmd_option_exists(argv, argv_end, "--estimation")) {
    result_log << "total_count,distinct_count,chunk_size,num_bins,column_name,predicate_condition,value,actual_count,"
                  "equal_height_hist_count,equal_distinct_count_hist_count,equal_width_hist_count\n";
    memory_log << "column_name,bin_count,bin_id,equal_height_hist,equal_distinct_count_hist,equal_width_hist\n";
  } else if (cmd_option_exists(argv, argv_end, "--pruning")) {
    result_log << "total_count,distinct_count,chunk_size,num_bins,column_name,predicate_condition,value,prunable,"
                  "equal_height_hist_prunable,equal_distinct_count_hist_prunable,equal_width_hist_prunable\n";
    memory_log << "column_name,bin_count,bin_id,equal_height_hist,equal_distinct_count_hist,equal_width_hist\n";
  } else if (cmd_option_exists(argv, argv_end, "--estimation-cqf")) {
    result_log << "total_count,distinct_count,chunk_size,num_bins,column_name,predicate_condition,value,actual_count,"
                  "cqf_count,equal_height_hist_count,equal_distinct_count_hist_count,equal_width_hist_count\n";
    memory_log << "column_name,bin_count,bin_id,cqf,equal_height_hist,equal_distinct_count_hist,equal_width_hist\n";
  } else if (cmd_option_exists(argv, argv_end, "--pruning-cqf")) {
    result_log << "total_count,distinct_count,chunk_size,num_bins,column_name,predicate_condition,value,prunable,"
                  "cqf_prunable,equal_height_hist_prunable,equal_distinct_count_hist_prunable,"
                  "equal_width_hist_prunable\n";
    memory_log << "column_name,bin_count,bin_id,cqf,equal_height_hist,equal_distinct_count_hist,equal_width_hist\n";
  } else if (cmd_option_exists(argv, argv_end, "--estimation-minmaxdistinct")) {
    result_log
        << "total_count,distinct_count,chunk_size,num_bins,column_name,predicate_condition,value,actual_count,"
           "min_max_distinct_count,equal_height_hist_count,equal_distinct_count_hist_count,equal_width_hist_count\n";
    memory_log << "column_name,bin_count,bin_id,min_max_distinct,equal_height_hist,equal_distinct_count_hist,equal_"
                  "width_hist\n";
  } else if (cmd_option_exists(argv, argv_end, "--pruning-minmax")) {
    result_log << "total_count,distinct_count,chunk_size,num_bins,column_name,predicate_condition,value,prunable,"
                  "min_max_prunable,equal_height_hist_prunable,equal_distinct_count_hist_prunable,"
                  "equal_width_hist_prunable\n";
    memory_log << "column_name,bin_count,bin_id,min_max,equal_height_hist,equal_distinct_count_hist,equal_width_hist\n";
  } else if (cmd_option_exists(argv, argv_end, "--pruning-range")) {
    result_log << "total_count,distinct_count,chunk_size,num_bins,column_name,predicate_condition,value,prunable,"
                  "range_prunable,equal_height_hist_prunable,equal_distinct_count_hist_prunable,"
                  "equal_width_hist_prunable\n";
    memory_log << "column_name,bin_count,bin_id,range,equal_height_hist,equal_distinct_count_hist,equal_width_hist\n";
  } else if (cmd_option_exists(argv, argv_end, "--time-estimation")) {
    result_log << "bin_count,iteration_count,column_name,predicate_condition,value,"
                  "minmaxdistinct_time,cqf_time,height_time,distinct_time,width_time\n";
  } else {
    Fail("Specify either '--estimation', '--estimation-cqf', or '--pruning' to decide what to measure.");
  }

  std::ofstream bin_log = std::ofstream(output_path + "/bins.log", std::ios_base::out | std::ios_base::trunc);

  // Data type and parameters of function call do not matter here.
  bin_log << AbstractHistogram<int32_t>::bins_to_csv_header("column_name", 0, ChunkID{0});

  CsvMeta csv_meta;
  if (cmd_option_exists(argv, argv_end, "--meta-path")) {
    csv_meta = process_csv_meta_file(get_cmd_option<std::string>(argv, argv_end, "--meta-path"));
  }

  for (const auto chunk_size : chunk_sizes) {
    log("Loading table with chunk_size " + std::to_string(chunk_size) + "...");

    std::shared_ptr<const Table> table;

    if (cmd_option_exists(argv, argv_end, "--meta-path")) {
      csv_meta.chunk_size = chunk_size;

      auto importer = std::make_shared<ImportCsv>(table_path, csv_meta);
      importer->execute();
      table = importer->get_output();
    } else if (cmd_option_exists(argv, argv_end, "--binary")) {
      auto importer = std::make_shared<ImportBinary>(table_path);
      importer->execute();
      table = importer->get_output();
    } else {
      table = load_table(table_path, chunk_size);
    }

    if (cmd_option_exists(argv, argv_end, "--estimation")) {
      run_estimation(table, num_bins_list, filters, result_log, bin_log, memory_log);
    } else if (cmd_option_exists(argv, argv_end, "--estimation-cqf")) {
      run_estimation_cqf(table, num_bins_list, filters, result_log, bin_log, memory_log);
    } else if (cmd_option_exists(argv, argv_end, "--estimation-minmaxdistinct")) {
      run_estimation_minmaxdistinct(table, num_bins_list, filters, result_log, bin_log, memory_log);
    } else if (cmd_option_exists(argv, argv_end, "--pruning")) {
      run_pruning(table, num_bins_list, filters, result_log, bin_log, memory_log);
    } else if (cmd_option_exists(argv, argv_end, "--pruning-cqf")) {
      run_pruning_cqf(table, num_bins_list, filters, result_log, bin_log, memory_log);
    } else if (cmd_option_exists(argv, argv_end, "--pruning-minmax")) {
      run_pruning_minmax(table, num_bins_list, filters, result_log, bin_log, memory_log);
    } else if (cmd_option_exists(argv, argv_end, "--pruning-range")) {
      run_pruning_range(table, num_bins_list, filters, result_log, bin_log, memory_log);
    } else if (cmd_option_exists(argv, argv_end, "--time-estimation")) {
      time_estimation(table, num_bins_list, filters, result_log);
    }
  }

  log("Done.");

  return 0;
}
