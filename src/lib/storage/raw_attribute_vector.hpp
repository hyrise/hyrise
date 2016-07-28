#include "base_attribute_vector.hpp"

namespace opossum {

template<typename T>
class raw_attribute_vector : public base_attribute_vector {
	std::vector<T> _values;

	virtual void insert(all_type_variant val) {
		_values.push_back(type_cast<T>(val));
	}

	virtual void print(long row) {
		std::cout << _values[row];
	}

	virtual std::string type_name() {
		return typeid(T).name();
	}

	virtual void print_all_positions(all_type_variant param) {
		T real_param = type_cast<T>(param);
		for(size_t i = 0; i < _values.size(); ++i) {
			if(_values[i] == real_param) std::cout << "pos " << i << std::endl;
		}
	}
};

}