#pragma once

#include "../common.hpp"
#include "table.hpp"

namespace opossum {

class table;


// A chunk has an chunk_id
// Items in a chunk have a chunk_row_id (starting at 0 for every chunk)
// Both combined are called recordID

// A pos_list_t holds a pointer to the corresponding table and maps chunk_ids to (qualifying) chunk_row_ids

class pos_list_t {
public:
    pos_list_t(const table* table) : _table(table) {};

    // unnecessary
    void add_chunk(chunk_id_t chunk_id, chunk_row_id_list_t&& chunk_row_id_list) {
        _record_ids.emplace(chunk_id, chunk_row_id_list);
    }

    void print() DEV_ONLY {
        for (auto const& record_id : _record_ids) {
            chunk_id_t chunk_id = record_id.first;
            chunk_row_id_list_t chunk_row_id_list = record_id.second;
            std::cout << "Chunk ID: " << chunk_id << " Chunk Row IDs: ";
            for (auto const& chunk_row_id : chunk_row_id_list) {
                std::cout << chunk_row_id << " ";
            }
            std::cout << std::endl;
        }
    }
    // TODO shared_ptr
    const table* _table;
    std::map<chunk_id_t, chunk_row_id_list_t> _record_ids;
};

}
