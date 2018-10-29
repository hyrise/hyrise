#include "abstract_statistics_object.hpp"

namespace opossum {

std::shared_ptr<AbstractStatisticsObject> AbstractStatisticsObject::_reduce_to_single_bin_histogram_impl() const {
  return nullptr;
}

}  // namespace opossum
