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

    virtual pos_list_t execute(pos_list_t pos_list, size_t column_id) const = 0;
};

}