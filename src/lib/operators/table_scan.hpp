#pragma once

#include "abstract_operator.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

template<typename T>
class table_scan_impl;

class table_scan : public abstract_operator {
public:
	table_scan(const std::shared_ptr<abstract_operator> in, const std::string &filter_column_name,/* const std::string &op,*/ const all_type_variant value);
	virtual void execute();
    virtual std::shared_ptr<table> get_output() const;

protected:
    virtual const std::string get_name() const;
    virtual short get_num_in_tables() const;
    virtual short get_num_out_tables() const;

    const std::unique_ptr<abstract_operator_impl> _impl;
};

template<typename T>
class table_scan_impl : public abstract_operator_impl {
public:
	table_scan_impl(const std::shared_ptr<abstract_operator> in, const std::string &filter_column_name, /*const std::string &op,*/ const all_type_variant value) :
		_filter_value(type_cast<T>(value)),
		_table(in->get_output()),
		_filter_column_id(_table->get_column_id_by_name(filter_column_name)),
		_output(new table),
		_pos_list(new pos_list)
		{
			for(size_t column_id = 0; column_id < _table->col_count(); ++column_id) {
				auto ref = std::make_shared<reference_column>(_table, column_id, _pos_list);
				_output->add_column(_table->get_column_name(column_id), _table->get_column_type(column_id), false);
				_output->get_chunk(0).add_column(ref);
				// TODO do we want to distinguish between chunk tables and "reference tables"?
			}
		}

	void execute() {
		size_t first_pos_in_current_chunk = 0;
		for(size_t chunk_id = 0; chunk_id < _table->chunk_count(); ++chunk_id) {
			auto &chunk = _table->get_chunk(chunk_id);
			auto base_column = chunk.get_column(_filter_column_id);

			if (auto val_col = std::dynamic_pointer_cast<value_column<T>>(base_column) ) {
				// value_column
				const std::vector<T> &values = val_col->get_values();
				for(size_t pos_in_column = 0; pos_in_column < chunk.size(); ++pos_in_column) {
					if (values[pos_in_column] == _filter_value) {
						_pos_list->emplace_back(first_pos_in_current_chunk + pos_in_column);
					}
				}
			} else {
				// reference_column
				auto ref_col = std::dynamic_pointer_cast<reference_column>(base_column);
				auto pos_list = ref_col->get_pos_list();

				// TODO: improve when chunk can be derived from position
				for(size_t pos_in_poslist = 0; pos_in_poslist < ref_col->size(); ++pos_in_poslist) {
					if (type_cast<T>((*ref_col)[(*pos_list)[pos_in_poslist]]) == _filter_value) {
						_pos_list->emplace_back((*pos_list)[pos_in_poslist]);
					}
				}

			}

			first_pos_in_current_chunk += chunk.size();

			// // OLD
			// for(size_t pos_in_column = 0; pos_in_column < chunk.size(); ++pos_in_column) {
			// 	if(type_cast<T>((*base_column)[pos_in_column]) == _filter_value) {
			// 		// TODO optimize. base_column::operator[] is DEV_ONLY. We should cast base_column to the actual type for performance reasons
			// 		// remember that it could be a reference_column itself...
			// 		_pos_list->emplace_back(first_pos_in_current_chunk + pos_in_column);
			// 	}
			// }

			// first_pos_in_current_chunk += chunk.size();
		}
	}

	virtual std::shared_ptr<table> get_output() const {
		return _output;
	}

	const T _filter_value;
	const std::shared_ptr<table> _table;
	const size_t _filter_column_id;
	std::shared_ptr<table> _output;
	std::shared_ptr<pos_list> _pos_list;
};

}