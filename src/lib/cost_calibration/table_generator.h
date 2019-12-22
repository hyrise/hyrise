#include "types.hpp"

#include <iostream>
#include <all_type_variant.hpp>
#include <storage/encoding_type.hpp>
#include <synthetic_table_generator.hpp>

/*
#include <boost/fusion/adapted/struct.hpp>
#include <boost/fusion/include/for_each.hpp>
#include <boost/phoenix/phoenix.hpp>
*/

#include "types.hpp"

class Table;

namespace opossum {

    struct TableGeneratorConfig{
        std::set<DataType> data_types;
        std::set<EncodingType> encoding_types;
        std::vector<ColumnDataDistribution> column_data_distribution;
    };

    class TableGenerator {
        public:
            TableGenerator(std::shared_ptr<TableGeneratorConfig> config);
            std::shared_ptr<Table> generateTable(const size_t row_count, const ChunkOffset chunk_size);

        private:
            std::vector<DataType> data_types_collection;
            std::vector<SegmentEncodingSpec> segment_encoding_spec_collection;
            std::vector<ColumnDataDistribution> column_data_distribution_collection;
            std::vector<std::string> column_names;
        };

}

/*
BOOST_FUSION_ADAPT_STRUCT(opossum::TableGeneratorConfig,
        (std::shared_ptr<std::set<DataType>>, data_types)
        (std::shared_ptr<std::set<DataType>>, encoding_types)
        (std::shared_ptr<std::set<ColumnDataDistribution>>, column_data_distribution)
    )
*/