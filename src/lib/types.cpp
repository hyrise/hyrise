#include "types.hpp"

namespace opossum {

std::string to_string(const all_type_variant& x) {
	return boost::lexical_cast<std::string>(x);
}

}