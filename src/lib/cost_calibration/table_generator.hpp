#include "types.hpp"

#include <iostream>
#include <all_type_variant.hpp>
#include <storage/encoding_type.hpp>
#include <synthetic_table_generator.hpp>
#include "types.hpp"

class Table;

namespace opossum {

    struct TableGeneratorConfig{
        std::set<DataType> data_types;
        std::set<EncodingType> encoding_types;
        std::vector<ColumnDataDistribution> column_data_distribution;
        std::set<int> chunk_offsets;
        std::set<int> row_counts;
    };

    class TableGenerator {
        public:
            TableGenerator(std::shared_ptr<TableGeneratorConfig> config);
            std::vector<std::shared_ptr<Table>> generate() const;

        private:
            std::vector<DataType> data_types_collection;
            std::vector<SegmentEncodingSpec> segment_encoding_spec_collection;
            std::vector<ColumnDataDistribution> column_data_distribution_collection;
            std::vector<int> chunk_offsets;
            std::vector<int> row_counts;
            std::vector<std::string> column_names;
        };

}
