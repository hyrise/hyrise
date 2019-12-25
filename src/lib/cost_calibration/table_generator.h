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

/*
BOOST_FUSION_ADAPT_STRUCT(opossum::TableGeneratorConfig,
        (std::shared_ptr<std::set<DataType>>, data_types)
        (std::shared_ptr<std::set<DataType>>, encoding_types)
        (std::shared_ptr<std::set<ColumnDataDistribution>>, column_data_distribution)
    )
*/