#pragma once

#include <cstdint>
#include <string>

#include <boost/preprocessor/seq/for_each.hpp>

#define COLUMN_TYPES (int32_t)(int64_t)(float)(double)(std::string)

#define EXPLICIT_INSTANTIATION(r, __class__, __type__) \
  template class __class__<__type__>;

#define EXPLICITLY_INSTANTIATE_COLUMN_TYPES(__class__) BOOST_PP_SEQ_FOR_EACH(EXPLICIT_INSTANTIATION, __class__, COLUMN_TYPES)