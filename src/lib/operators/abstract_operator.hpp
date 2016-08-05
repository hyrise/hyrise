#include <vector>

#include "../common.hpp"
#include "../storage/table.hpp"

#pragma once

namespace opossum {

class abstract_operator {
public:
    abstract_operator() {}
    abstract_operator(abstract_operator const&) = delete;
    abstract_operator(abstract_operator&&) = default;

    virtual record_id_list_t execute(record_id_list_t record_id_list, size_t column_id) const = 0;
};

}