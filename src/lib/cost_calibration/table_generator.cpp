//
// Created by Lukas Böhme on 17.12.19.
//

#include "table_generator.h"
#include "storage/table.hpp"

namespace opossum {

    TableGenerator::TableGenerator(std::shared_ptr<TableGeneratorConfig> config) {
        ColumnID column_number {0};
        for (DataType data_type : config->data_types){
            for (EncodingType encoding_type : config->encoding_types){
                for (ColumnDataDistribution column_data_distribution : config->column_data_distribution){
                    data_types_collection.emplace_back(data_type);
                    segment_encoding_spec_collection.emplace_back(SegmentEncodingSpec(encoding_type));
                    column_data_distribution_collection.emplace_back(column_data_distribution);
                    column_names.emplace_back(std::to_string(column_number));

                    //std::stringstream ss;
                    //ss << encoding?ztpe ;
                    //ss.str(),
                }
            }
        }
    }

    std::shared_ptr<Table> TableGenerator::generateTable(const size_t row_count, const ChunkOffset chunk_size) {
        auto table_generator = std::make_shared<SyntheticTableGenerator>();

        auto table = table_generator->generate_table(
                column_data_distribution_collection,
                data_types_collection,
                row_count,
                chunk_size,
                {segment_encoding_spec_collection},
                {column_names},
                UseMvcc::Yes    // MVCC = Multiversion concurrency control
        );

        return table;
    }
}