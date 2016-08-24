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
			// if(DEBUG) {
			// 	auto ref_column = _referenced_table->get_chunk(0).get_column(referenced_column_id);
			// 	// TODO cannot use template parameter here
			// 	auto val_col = dynamic_cast<value_column<T>>(ref_column);
			// 	if (val_col == NULL) {
			// 		throw std::logic_error("reference_column must be value_column");
			// 	}
			// }
		}

	virtual all_type_variant operator[](const size_t i) const DEV_ONLY {
		// TODO chunk anhand des ersten Teils der Pos-ID (Zweierpotenz) identifizieren
		size_t pos_in_rest_of_table = (*_pos_list)[i];
		for(size_t chunk_id = 0; chunk_id < _referenced_table->chunk_count(); ++chunk_id) {
			auto &chunk = _referenced_table->get_chunk(chunk_id);
			if(chunk.size() > pos_in_rest_of_table) {
				return (*chunk.get_column(_referenced_column_id))[pos_in_rest_of_table];
			} else {
				pos_in_rest_of_table -= chunk.size();
			}
		}
		throw std::runtime_error("Position not found");
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