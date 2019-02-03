
#include <memory>
#include <random>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

#include "boost/math/distributions/pareto.hpp"
#include "boost/math/distributions/skew_normal.hpp"
#include "boost/math/distributions/uniform.hpp"

#include "types.hpp"

#include "column_generator.hpp"
#include "table_generator.hpp"

#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

ColumnGenerator::ColumnGenerator() : _probability_dist(0.0, 1.0) {
  std::random_device rd;
  _pseudorandom_engine.seed(rd());
}

/**
 * Generates a value vector of type int according to the given column_data distribution. The values are selected
 * randomly.
 * @tparam VectorType Type of vector. Could be std::vector<int> or tbb::concurrent_vector<int>
 * @param column_data_distribution Distribution of values.
 * @param row_count Number of rows to calculate
 * @param allow_value Function which checks, if the random value may be added to the returned vector.
 * @return A vector with row_count randomly selected integers for which allow_value returned true.
 */
// Todo: Refactor TableGenerator::generate_table to uses this function
template <typename VectorType>
VectorType ColumnGenerator::generate_value_vector(const ColumnDataDistribution& column_data_distribution,
                                                  size_t row_count, const std::function<bool(int)>& allow_value) {
  VectorType result(row_count);

  auto generate_value_by_distribution_type = std::function<int(void)>{};

  // generate distribution from column configuration
  switch (column_data_distribution.distribution_type) {
    case DataDistributionType::Uniform: {
      auto uniform_dist = boost::math::uniform_distribution<double>{column_data_distribution.min_value,
                                                                    column_data_distribution.max_value};
      generate_value_by_distribution_type = [&]() {
        const auto probability = _probability_dist(_pseudorandom_engine);
        return static_cast<int>(std::floor(boost::math::quantile(uniform_dist, probability)));
      };
      break;
    }
    case DataDistributionType::NormalSkewed: {
      auto skew_dist = boost::math::skew_normal_distribution<double>{column_data_distribution.skew_location,
                                                                     column_data_distribution.skew_scale,
                                                                     column_data_distribution.skew_shape};
      generate_value_by_distribution_type = [&]() {
        const auto probability = _probability_dist(_pseudorandom_engine);
        return static_cast<int>(std::round(boost::math::quantile(skew_dist, probability) * 10));
      };
      break;
    }
    case DataDistributionType::Pareto: {
      auto pareto_dist = boost::math::pareto_distribution<double>{column_data_distribution.pareto_scale,
                                                                  column_data_distribution.pareto_shape};
      generate_value_by_distribution_type = [&]() {
        const auto probability = _probability_dist(_pseudorandom_engine);
        return static_cast<int>(std::floor(boost::math::quantile(pareto_dist, probability)));
      };
      break;
    }
  }

  for (size_t row_offset{0}; row_offset < row_count; ++row_offset) {
    auto rnd_value = generate_value_by_distribution_type();
    while (!allow_value(rnd_value)) {
      rnd_value = generate_value_by_distribution_type();
    }
    result[row_offset] = rnd_value;
  }

  return result;
}

/**
 * Generates a vector to act as a join partner. The generated vector will contain selectivity * row_count many values
 * which will find a join partner in the vector join_partner. Values, which must not find a join partner will be
 * selected using get_value_with_no_join_partner.
 * @param join_partner Column which will act as a join partner for the generated vector
 * @param row_count Number of rows to generate
 * @param selectivity Value in closed interval [0, 1] to indicate how many generated values will find a join partner in
 * join_partner.
 * @param get_value_with_no_join_partner Function to compute a value which is not found in join_partner
 * @return Vector with selectivit * row_count many join partners to join_partner
 */
std::vector<int> ColumnGenerator::generate_join_partner(
    const std::vector<int>& join_partner, size_t row_count, double selectivity,
    const std::function<int(double)>& get_value_with_no_join_partner) {
  std::vector<int> result(row_count);
  auto required_join_partners = static_cast<size_t>(selectivity * row_count);
  auto required_no_join_partners = row_count - required_join_partners;
  const auto select_join_partner_threshold = required_join_partners / row_count;

  size_t selected_rows = 0;
  while (selected_rows < row_count) {
    auto rnd = _probability_dist(_pseudorandom_engine);
    if ((required_join_partners > 0 && rnd < select_join_partner_threshold) || required_no_join_partners == 0) {
      const auto rnd_join_index = static_cast<size_t>(_probability_dist(_pseudorandom_engine) * join_partner.size());
      result[selected_rows] = join_partner[rnd_join_index];
      --required_join_partners;
    } else {
      result[selected_rows] = get_value_with_no_join_partner(_probability_dist(_pseudorandom_engine));
      --required_no_join_partners;
    }
    ++selected_rows;
  }

  return result;
}

/**
 * Generates a pair of two tables. The tables are generated in a way, that the columns of table 1 can be joined
 * with the columns of table 2 with a given selectivity.
 * @param selectivities Join selectivity in the closed interval [0, 1]. The index i of the selectivity entry will be
 * used to calculate how many entries of column i in table 2 will find a join partner in column i of table 1.
 * @param chunk_size Chunk size of tables
 * @param row_count_table1 Number of rows to be calculated for table 1
 * @param row_count_table2 Number of rows to be calculated for table 2
 * @param min_value Minimum value to be calculated
 * @param max_value Maximum value to be calculated
 * @param allow_value Function to determine if a calculted value can be added to table 1.
 * @param get_value_with_no_join_partner Function to compute a value for table 2 which is not present in the
 * corresponding column of table 1.
 * @return
 */
std::unique_ptr<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>>
ColumnGenerator::generate_joinable_table_pair(const std::vector<double>& selectivities, size_t chunk_size,
                                              size_t row_count_table1, size_t row_count_table2, uint32_t min_value,
                                              uint32_t max_value, const std::function<bool(int)>& allow_value,
                                              const std::function<int(double)>& get_value_without_join_partner) {
  const auto column_count = selectivities.size();

  TableColumnDefinitions column_definitions;
  for (size_t i = 0; i < column_count; i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    column_definitions.emplace_back(column_name, DataType::Int);
  }

  const auto value_distribution = ColumnDataDistribution::make_uniform_config(min_value, max_value);

  std::vector<std::vector<int>> cols_table1;
  std::vector<std::vector<int>> cols_table2;
  cols_table1.resize(column_count);
  cols_table2.resize(column_count);

  // TODO(anyone)
  // const auto allow_value = [](size_t value) { return value % 10 != 0; };
  // const auto get_value_without_join_partner = [&max_value](double value)
  // { return static_cast<int>(value * max_value / 10) * 10; };

  for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
    const auto values_table1 =
        generate_value_vector<std::vector<int>>(value_distribution, row_count_table1, allow_value);
    cols_table1[col_idx] = std::move(values_table1);

    const auto values_table2 = generate_join_partner(cols_table1[col_idx], row_count_table2, selectivities[col_idx],
                                                     get_value_without_join_partner);
    cols_table2[col_idx] = std::move(values_table2);
  }

  const auto table1 = create_table(column_definitions, chunk_size, cols_table1);
  const auto table2 = create_table(column_definitions, chunk_size, cols_table2);

  return std::make_unique<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>>(
      std::make_pair(std::move(table1), std::move(table2)));
}

/**
 * Creates a table given the column definitions and table_data.
 * @param column_definitions
 * @param chunk_size
 * @param table_data
 * @return
 */
std::shared_ptr<Table> ColumnGenerator::create_table(const TableColumnDefinitions& column_definitions,
                                                     size_t chunk_size,
                                                     const std::vector<std::vector<int>>& table_data) {
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);
  const auto row_count = table_data.front().size();
  const auto col_count = table_data.size();
  for (size_t row_index = 0; row_index < row_count; ++row_index) {
    auto row = std::vector<AllTypeVariant>(col_count);
    for (size_t col_index = 0; col_index < col_count; ++col_index) {
      row[col_index] = table_data[col_index][row_index];
    }
    table->append(row);
  }
  return table;
}

// we could use this to set visibility of mvcc
void set_all_records_visible(Table& table) {
  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto chunk = table.get_chunk(chunk_id);
    auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

    for (auto i = 0u; i < chunk->size(); ++i) {
      mvcc_data->begin_cids[i] = 0u;
      mvcc_data->end_cids[i] = MvccData::MAX_COMMIT_ID;
    }
  }
}

std::unique_ptr<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>>
ColumnGenerator::generate_two_predicate_join_tables(size_t chunk_size, size_t fact_table_size, size_t fact_factor,
                                                    double probing_factor) {
  DebugAssert(fact_table_size % fact_factor == 0, "fact factor must be a factor of fact_table_size.");

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("t1_a", DataType::Int);
  column_definitions.emplace_back("t1_b", DataType::Int);
  auto fact_table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);

  column_definitions.clear();
  column_definitions.emplace_back("t2_a", DataType::Int);
  column_definitions.emplace_back("t2_b", DataType::Int);
  auto probe_table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);

  const int fact_value_upper_bound = static_cast<int>((fact_table_size - 1) / fact_factor);

  int col_b_value_tbl1 = 0;
  int col_b_value_tbl2 = 0;
  const size_t idk = static_cast<size_t>(fact_factor * fact_factor * probing_factor);

  for (int fact_value = 0; fact_value <= fact_value_upper_bound; ++fact_value) {
    for (size_t row_cnt = 0; row_cnt < fact_factor; ++row_cnt) {
      col_b_value_tbl1 %= (fact_factor);
      fact_table->append({fact_value, col_b_value_tbl1});
      ++col_b_value_tbl1;
    }

    for (size_t row_cnt = 0; row_cnt < idk; ++row_cnt) {
      col_b_value_tbl2 %= (fact_factor);
      probe_table->append({fact_value, col_b_value_tbl2});
      ++col_b_value_tbl2;
    }
  }

  // see line 205
  set_all_records_visible(*fact_table);
  set_all_records_visible(*probe_table);

  return std::make_unique<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>>(
      std::make_pair(std::move(fact_table), std::move(probe_table)));
}

}  // namespace opossum
