#pragma once

#include "base_column.hpp"
#include "table.hpp"

#include <vector>
#include <iostream>

namespace opossum {

class reference_column : public base_column {
// TODO move implementation to CPP


protected:
	const std::shared_ptr<table> _referenced_table;
	const size_t _referenced_column_id;
	const std::shared_ptr<pos_list> _pos_list;

public:
	reference_column(const std::shared_ptr<table> referenced_table,
		const size_t referenced_column_id,
		const std::shared_ptr<pos_list> pos)
		: _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos)
		{
			if(DEBUG) {
				auto referenced_column = _referenced_table->get_chunk(0).get_column(referenced_column_id);
				auto reference_col = std::dynamic_pointer_cast<reference_column>(referenced_column);
				if (reference_col != NULL) {
					// cast was successful, but was expected to fail
					throw std::logic_error("referenced_column must not be a reference_column");
				}
			}
		}

	virtual all_type_variant operator[](const size_t i) const DEV_ONLY {
		auto &chunk = _referenced_table->get_chunk(get_chunk_id_from_row_id(i));
		return (*chunk.get_column(_referenced_column_id))[get_chunk_offset_from_row_id(i)];
	}

	virtual void append(const all_type_variant&) {
		throw std::logic_error("reference_column is immutable");
	}

	virtual size_t size() const {
		return _pos_list->size();
	}

	const std::shared_ptr<pos_list> get_pos_list() const {
		return _pos_list;
	}

};

// TODO: Dokumentieren, dass nicht alle Chunks einer Tabelle gleich groß sind.
// Wenn man einen Union aus zwei Tabellen mit jeweils einem Chunk macht, entstehenden zwei reference_columns, die auf verschiedene Tabellen
// verweisen und unterschiedlich lang sind

}