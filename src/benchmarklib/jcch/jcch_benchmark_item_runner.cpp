#include "jcch_benchmark_item_runner.hpp"

#include <fstream>
#include <random>

#include "tpch/tpch_queries.hpp"
#include "utils/string_utils.hpp"
#include "utils/timer.hpp"

namespace opossum {

JCCHBenchmarkItemRunner::JCCHBenchmarkItemRunner(const bool skewed, const std::string& dbgen_path,
                                                 const std::string& data_path,
                                                 const std::shared_ptr<BenchmarkConfig>& config,
                                                 bool use_prepared_statements, float scale_factor)
    : TPCHBenchmarkItemRunner(config, use_prepared_statements, scale_factor),
      _skewed(skewed),
      _dbgen_path(dbgen_path),
      _data_path(data_path) {
  _load_params();
}

JCCHBenchmarkItemRunner::JCCHBenchmarkItemRunner(const bool skewed, const std::string& dbgen_path,
                                                 const std::string& data_path,
                                                 const std::shared_ptr<BenchmarkConfig>& config,
                                                 bool use_prepared_statements, float scale_factor,
                                                 const std::vector<BenchmarkItemID>& items)
    : TPCHBenchmarkItemRunner(config, use_prepared_statements, scale_factor, items),
      _skewed(skewed),
      _dbgen_path(dbgen_path),
      _data_path(data_path) {
  _load_params();
}

std::string JCCHBenchmarkItemRunner::item_name(const BenchmarkItemID item_id) const {
  Assert(item_id < 22u, "item_id out of range");
  return std::string("JCC-H ") + (_skewed ? "(skewed) " : "(normal) ") + (item_id + 1 < 10 ? "0" : "") +
         std::to_string(item_id + 1);
}

void JCCHBenchmarkItemRunner::_load_params() {
  const auto local_queries_path = _data_path + "/queries/";
  const auto params_path = local_queries_path + "params-" + (_skewed ? "skewed" : "normal");

  // Check if the query parameters have already been generated
  if (!std::filesystem::exists(params_path)) {
    Timer timer;

    std::cout << "- Creating query parameters by calling external qgen" << std::flush;

    // Check for the existence of dbgen's query templates (1.sql etc.) at the expected location
    const auto dbgen_queries_path = _dbgen_path + "/queries/";
    Assert(std::filesystem::exists(dbgen_queries_path),
           std::string{"Query templates not found at "} + dbgen_queries_path);

    // Create local directory and copy query templates if needed
    const auto local_queries_dir_created = std::filesystem::create_directory(local_queries_path);
    Assert(std::filesystem::exists(local_queries_path), "Creating JCC-H queries folder failed");
    if (local_queries_dir_created) {
      auto cmd = std::stringstream{};
      cmd << "cd " << local_queries_path << " && ln -s " << _dbgen_path << "/queries/*.sql .";
      auto ret = system(cmd.str().c_str());
      Assert(!ret, "Creating symlinks to query templates failed");
    }

    // Call qgen a couple of times with different PRNG seeds and store the resulting query parameters in queries/params.
    // dbgen doesn't like `-r 0`, so we start at 1.
    for (auto seed = 1; seed <= (_config->max_runs > 0 ? _config->max_runs : 100'000); ++seed) {
      auto cmd = std::stringstream{};
      cmd << "cd " << local_queries_path << " && " << _dbgen_path << "/qgen " << (_skewed ? "-k" : "") << " -s "
          << _scale_factor << " -b " << _dbgen_path << "/dists.dss -r " << seed << " -l " << params_path
          << " >/dev/null";
      auto ret = system(cmd.str().c_str());
      Assert(!ret, "Calling qgen failed");
    }

    std::cout << " (" << timer.lap_formatted() << ")" << std::endl;
  }

  // Open the params file, which looks like this:
  //   query_id|param0|param1
  auto file = std::ifstream(params_path);
  Assert(file.is_open(), std::string{"Could not open JCC-H parameters at "} + params_path);

  std::string line;
  while (std::getline(file, line)) {
    // Load the parameter into the corresponding entry in _all_params
    auto string_values = split_string_by_delimiter(line, '\t');
    const auto query_id = std::stoi(string_values[0]);
    Assert(query_id >= 1 && query_id <= 22, "Invalid query_id");
    string_values.erase(string_values.begin());
    _all_params[query_id - 1].emplace_back(string_values);
  }
}

bool JCCHBenchmarkItemRunner::_on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) {
  using namespace std::string_literals;  // NOLINT

  const auto& this_item_params = _all_params[item_id];

  // Choose a random parameterization from _all_params
  static thread_local std::minstd_rand random_engine{_random_seed++};
  std::uniform_int_distribution<> params_dist{0, static_cast<int>(this_item_params.size() - 1)};
  const auto raw_params_iter = this_item_params.begin() + params_dist(random_engine);

  std::vector<std::string> parameters;
  auto sql = std::string{};

  // This mirrors TPCHBenchmarkItemRunner::_on_execute_item. Instead of generating random parameters according to the
  // TPC-H specifications, it uses the ones generated by JCC-H's qgen
  switch (item_id) {
    // Writing `1-1` to make people aware that this is zero-indexed while TPC-H query names are not
    case 1 - 1: {
      // In some cases, we still need to do the date calculations that Hyrise does not support yet
      const auto date = _calculate_date(boost::gregorian::date{1998, 12, 01}, 0, -std::stoi(raw_params_iter->at(0)));
      parameters.emplace_back("'"s + date + "'");
      break;
    }

    case 2 - 1: {
      parameters.emplace_back(raw_params_iter->at(0));
      parameters.emplace_back("'%"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(2) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(2) + "'");
      break;
    }

    case 3 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      break;
    }

    case 4 - 1: {
      const auto begin_date = boost::gregorian::from_string(raw_params_iter->at(0));
      const auto end_date_str = _calculate_date(begin_date, 3);

      // Cannot use begin_date here, as we would have to convert it into a string first.
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + end_date_str + "'");
      break;
    }

    case 5 - 1: {
      const auto begin_date = boost::gregorian::from_string(raw_params_iter->at(1));
      const auto end_date_str = _calculate_date(begin_date, 12);

      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + end_date_str + "'");
      break;
    }

    case 6 - 1: {
      const auto begin_date = boost::gregorian::from_string(raw_params_iter->at(0));
      const auto end_date_str = _calculate_date(begin_date, 12);

      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + end_date_str + "'");
      parameters.emplace_back(raw_params_iter->at(1));
      parameters.emplace_back(raw_params_iter->at(1));
      parameters.emplace_back(raw_params_iter->at(2));
      break;
    }

    case 7 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(2) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(3) + "'");
      break;
    }

    case 8 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(2) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(3) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(4) + "'");
      break;
    }

    case 9 - 1: {
      static auto warned_performance = false;
      if (!warned_performance) {
        std::cerr << "\nWarning: JCC-H Query 9 needs optimization. Consider skipping it using -q\n\n";
        warned_performance = true;
      }

      parameters.emplace_back("'%"s + raw_params_iter->at(0) + "%'");
      break;
    }

    case 10 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      break;
    }

    case 11 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back(raw_params_iter->at(1));
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      break;
    }

    case 12 - 1: {
      const auto begin_date = boost::gregorian::from_string(raw_params_iter->at(2));
      const auto end_date_str = _calculate_date(begin_date, 12);

      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(2) + "'");
      parameters.emplace_back("'"s + end_date_str + "'");
      break;
    }

    case 13 - 1: {
      parameters.emplace_back("'%"s + raw_params_iter->at(0) + '%' + raw_params_iter->at(1) + "%'");
      break;
    }

    case 14 - 1: {
      const auto begin_date = boost::gregorian::from_string(raw_params_iter->at(0));
      const auto end_date_str = _calculate_date(begin_date, 1);

      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + end_date_str + "'");
      break;
    }

    case 15 - 1: {
      auto query_15 = std::string{tpch_queries.at(15)};

      const auto begin_date = boost::gregorian::from_string(raw_params_iter->at(0));
      const auto end_date_str = _calculate_date(begin_date, 3);

      // Hack: We cannot use prepared statements in TPC-H 15. Thus, we need to build the SQL string by hand.
      // By manually replacing the `?` from tpch_queries.cpp, we can keep all queries in a readable form there.
      // This is ugly, but at least we can assert that nobody tampered with the string over there.
      static constexpr auto BEGIN_DATE_OFFSET = 156;
      static constexpr auto END_DATE_OFFSET = 192;
      DebugAssert((std::string_view{&query_15[BEGIN_DATE_OFFSET], 10} == "1996-01-01" &&
                   std::string_view{&query_15[END_DATE_OFFSET], 10} == "1996-04-01"),
                  "TPC-H 15 string has been modified");
      query_15.replace(BEGIN_DATE_OFFSET, 10, raw_params_iter->at(0));
      query_15.replace(END_DATE_OFFSET, 10, end_date_str);

      const auto view_id = std::atomic_fetch_add(&_q15_view_id, size_t{1});
      boost::replace_all(query_15, std::string("revenue_view"), std::string("revenue") + std::to_string(view_id));

      // Not using _substitute_placeholders here
      sql = query_15;
      break;
    }

    case 16 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      for (auto i = 0; i < 8; ++i) parameters.emplace_back(raw_params_iter->at(2 + i));
      break;
    }

    case 17 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      break;
    }

    case 18 - 1: {
      static auto warned_compliance = false;
      if (!warned_compliance) {
        std::cerr << "\nWarning: JCC-H Query 18 as used by Hyrise slightly diverges from the specification.\n";
        std::cerr << "         See jcch_benchmark_item_runner.cpp for details.\n\n";
        warned_compliance = true;
      }

      // JCC-H has a second parameter to this query:
      //   https://github.com/ldbc/dbgen.JCC-H/commit/d42a7ebc2617ec31de55b00425c23ab7885beeeb#diff-c448b6246f882ef1a5fd8e7ded77b8134addba8443ce2b43425e563045895fc4  // NOLINT
      // We do not use this parameter as it would bring a structural change to the SQL query template, which is also
      // used for TPC-H.
      parameters.emplace_back(raw_params_iter->at(0));
      break;
    }

    case 19 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      parameters.emplace_back(raw_params_iter->at(3));
      parameters.emplace_back(raw_params_iter->at(3));
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back(raw_params_iter->at(4));
      parameters.emplace_back(raw_params_iter->at(4));
      parameters.emplace_back("'"s + raw_params_iter->at(2) + "'");
      parameters.emplace_back(raw_params_iter->at(5));
      parameters.emplace_back(raw_params_iter->at(5));

      break;
    }

    case 20 - 1: {
      const auto begin_date = boost::gregorian::from_string(raw_params_iter->at(1));
      const auto end_date_str = _calculate_date(begin_date, 12);

      parameters.emplace_back("'"s + raw_params_iter->at(0) + "%'");
      parameters.emplace_back("'"s + raw_params_iter->at(1) + "'");
      parameters.emplace_back("'"s + end_date_str + "'");
      parameters.emplace_back("'"s + raw_params_iter->at(2) + "'");
      break;
    }

    case 21 - 1: {
      parameters.emplace_back("'"s + raw_params_iter->at(0) + "'");
      break;
    }

    case 22 - 1: {
      // We need the same country code twice - have a look at the query
      for (auto i = 0; i < 7; ++i) parameters.emplace_back("'"s + raw_params_iter->at(i) + "'");
      for (auto i = 0; i < 7; ++i) parameters.emplace_back("'"s + raw_params_iter->at(i) + "'");
      break;
    }

    default:
      Fail("There are only 22 JCC-H queries");
  }

  if (sql.empty()) sql = _substitute_placeholders(item_id, parameters);

  const auto [status, table] = sql_executor.execute(sql, nullptr);
  Assert(status == SQLPipelineStatus::Success, "JCC-H items should not fail");
  return true;
}

}  // namespace opossum
