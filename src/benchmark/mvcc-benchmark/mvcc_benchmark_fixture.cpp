#include "mvcc_benchmark_fixture.h"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "types.hpp"

namespace {
// Generating a table with 40,000 rows (see TableGenerator), a chunk size of 2,000 results in 20 chunks per table
    constexpr auto CHUNK_SIZE = opossum::ChunkID{2000};
    constexpr auto NUMBER_OF_ROWS = 40000;
}  // namespace

namespace opossum {

    void polluteTableWithUpdates(int updateCount) {

      // TODO increment values by one

    }

    void MVCC_Benchmark_Fixture::SetUp(::benchmark::State& state) {

        auto chunk_size = ChunkID(CHUNK_SIZE);
        auto table_generator = std::make_shared<TableGenerator>();

        // Generate a table with dummy data
        _table_name = "mvcc_table";

        // TODO Specify parameter of std::vector<ColumnDataDistribution>&
        auto table = table_generator->generate_table(column_data_distributions,NUMBER_OF_ROWS,CHUNK_SIZE);
        StorageManager::get().add_table(_table_name, table);

        // Invalidate rows
        polluteTableWithUpdates(static_cast<int>(state.range(0)));
    }

    void MVCC_Benchmark_Fixture::TearDown(::benchmark::State&) { StorageManager::reset(); }

    void MVCC_Benchmark_Fixture::_clear_cache() {
        std::vector<int> clear = std::vector<int>();
        clear.resize(500 * 1000 * 1000, 42);
        for (uint i = 0; i < clear.size(); i++) {
            clear[i] += 1;
        }
        clear.resize(0);
    }

}  // namespace opossum
