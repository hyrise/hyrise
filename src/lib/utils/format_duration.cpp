#include "format_duration.hpp"

#include <sstream>

#include <boost/hana/for_each.hpp>
#include <boost/hana/tuple.hpp>

#include "utils/assert.hpp"

namespace hyrise {

std::string format_duration(const std::chrono::nanoseconds& total_nanoseconds) {
  constexpr auto TIME_UNIT_ORDER =
      boost::hana::to_tuple(boost::hana::tuple_t<std::chrono::minutes, std::chrono::seconds, std::chrono::milliseconds,
                                                 std::chrono::microseconds, std::chrono::nanoseconds>);
  const std::vector<std::string> unit_strings = {" min", " s", " ms", " µs", " ns"};

  auto remaining_nanoseconds = total_nanoseconds;
  auto floor_durations = std::vector<uint64_t>{};
  auto round_durations = std::vector<uint64_t>{};
  boost::hana::for_each(TIME_UNIT_ORDER, [&](const auto duration_t) {
    using DurationType = typename decltype(duration_t)::type;
    auto floored_duration = std::chrono::floor<DurationType>(total_nanoseconds);
    floor_durations.emplace_back(floored_duration.count());
    round_durations.emplace_back(std::chrono::round<DurationType>(remaining_nanoseconds).count());
    remaining_nanoseconds -= floored_duration;
  });

  auto stream = std::stringstream{};
  const auto unit_string_count = unit_strings.size();
  for (auto unit_iterator = size_t{0}; unit_iterator < unit_string_count; ++unit_iterator) {
    const auto& floored_duration = floor_durations.at(unit_iterator);
    const auto is_last_element = unit_iterator == unit_strings.size() - 1;
    if (floored_duration > 0 || is_last_element) {
      stream << floored_duration << unit_strings.at(unit_iterator);
      if (!is_last_element) {
        stream << " " << round_durations.at(unit_iterator + 1) << unit_strings.at(unit_iterator + 1);
      }
      break;
    }
  }
  return stream.str();
}

}  // namespace hyrise
