#include "types.hpp"

namespace opossum {

class base_attribute_vector {
public:
	virtual void insert(all_type_variant val) = 0;
	virtual void print(long row) = 0;
	virtual std::string type_name() = 0;
	virtual void print_all_positions(all_type_variant param) = 0;
};

}