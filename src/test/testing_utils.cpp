#include "testing_utils.hpp"

namespace opossum {

std::string unique_random_name() { return to_string(boost::uuids::random_generator()()); }

}  // namespace opossum
