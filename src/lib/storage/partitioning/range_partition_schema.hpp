#pragma once

#include <storage/partitioning/partition_schema.hpp>
#include <types.hpp>
#include <all_type_variant.hpp>

namespace opossum {

class RangePartitionSchema : public PartitionSchema {

    ColumnID column_id;
    std::vector<AllTypeVariant> bounds;

};

}
