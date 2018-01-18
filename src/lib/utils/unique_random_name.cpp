#include "unique_random_name.hpp"

namespace opossum {

std::string unique_random_name() {
    return to_string(boost::uuids::uuid());
}

}  // namespace opossum
