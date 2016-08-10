#include "abstract_operator.hpp"

namespace opossum {

abstract_operator::abstract_operator(const std::shared_ptr<abstract_operator> left, const std::shared_ptr<abstract_operator> right) : 
	_input_left(left ? left->get_output() : nullptr),
	_input_right(right ? right->get_output() : nullptr) {
		
	}

}