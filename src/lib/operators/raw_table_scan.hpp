#pragma once

#include "abstract_operator.hpp"

namespace opossum {

template<typename T>
class raw_table_scan : public abstract_operator {
public:
    raw_table_scan(all_type_variant value) {
        _filter_value = type_cast<T>(value);
    };

    virtual record_id_list_t execute(record_id_list_t record_id_list, size_t column_id) const {
        auto r = record_id_list_t(record_id_list._table);
        for (auto const& record_id : record_id_list._record_ids) {
            std::vector<chunk_row_id_t> chunk_results;
            chunk_id_t chunk_id = record_id.first;
            chunk_row_id_list_t chunk_row_id_list = record_id.second;

            auto& chunk = record_id_list._table->get_chunk(chunk_id);

            auto chunk_column = std::dynamic_pointer_cast<raw_attribute_vector<T>>(chunk.get_column(column_id));
            auto values = chunk_column->get_values();

            for (auto const& chunk_row_id : chunk_row_id_list) {
                if (values[chunk_row_id] <= _filter_value) {
                    chunk_results.emplace_back(chunk_row_id);
                }
            }
            if (chunk_results.size()) {
                r.add_chunk(chunk_id, std::move(chunk_results));
            }
        }

        return r;

    }
protected:
    T _filter_value;
};

// TODO this should be a static method of raw_table_scan, but http://stackoverflow.com/a/8629943 and http://stackoverflow.com/a/17350254 TL;DR works only with gcc
abstract_operator* create_raw_table_scan(std::shared_ptr<table> table, size_t column_id, all_type_variant value) {
    return create_templated_2<abstract_operator, raw_table_scan>(table->get_column_type(column_id), value);
}

}