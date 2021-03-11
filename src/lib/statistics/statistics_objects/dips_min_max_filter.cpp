#include "min_max_filter.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "dips_min_max_filter.hpp"
#include "lossless_cast.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
DipsMinMaxFilter<T>::DipsMinMaxFilter(T init_min, T init_max, CommitID init_commitID)
    : MinMaxFilter<T>(init_min, init_max), commitID(init_commitID) {}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DipsMinMaxFilter);

}  // namespace opossum
