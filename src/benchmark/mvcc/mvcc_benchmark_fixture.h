#include <memory>
#include <storage/table.hpp>

#include "benchmark/benchmark.h"
#include "types.hpp"

namespace opossum {

    class TableWrapper;

// Defining the base fixture class
    class MVCC_Benchmark_Fixture : public benchmark::Fixture {
    public:
        void SetUp(::benchmark::State& state) override;
        void TearDown(::benchmark::State&) override;

    protected:
        void _clear_cache();
        void _invalidateRecords(int invalidatedRecordsCount);
        void _incrementAllValuesByOne();

    protected:
        std::string _table_name;
        std::shared_ptr<AbstractExpression> column_a;
    };

}  // namespace opossum
