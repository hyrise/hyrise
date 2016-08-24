#pragma once

#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>
#include <boost/hana/assert.hpp>
#include <boost/hana/core/make.hpp>
#include <boost/hana/integral_constant.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/type.hpp>
#include <boost/hana/string.hpp>
#include <boost/hana/for_each.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/ext/boost/mpl/vector.hpp>
#include <string>
#include <iostream>
#include <vector>
// TODO remove unused imports

namespace opossum {

namespace hana = boost::hana;


using RowID = uint64_t;    // first 32 bit for chunk_id  second 32 bit for chunk_offset
const uint32_t MAX_CHUNK_SIZE = 32;
using ChunkID = uint32_t;
using ChunkOffset = uint32_t;

ChunkID get_chunk_id_from_row_id(RowID r_id);
ChunkOffset get_chunk_offset_from_row_id(RowID r_id);
RowID get_row_id_from_chunk_id_and_chunk_offset(ChunkID c_id, ChunkOffset c_offset);


using pos_list = std::vector<RowID>;

// This holds all possible data types. The left side of the pairs are the names, the right side are prototypes ("examples").
// These examples are later used with decltype() in the template magic below.
static auto column_types = hana::make_tuple(
    hana::make_pair("int", 123),
    hana::make_pair("long", 123456789l),
    hana::make_pair("float", 123.4f),
    hana::make_pair("double", 123.4),
    hana::make_pair("string", std::string("hi"))
);

// convert tuple of all types to sequence by first extracting the prototypes only and then applying decltype_
static auto types_as_hana_sequence = hana::transform(hana::transform(column_types, hana::second), hana::decltype_);
// convert hana sequence to mpl vector
using types_as_mpl_vector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(types_as_hana_sequence));
// create boost::variant from mpl vector
using all_type_variant = typename boost::make_variant_over<types_as_mpl_vector>::type;

// cast methods - from variant to specific type
template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type
type_cast(all_type_variant value) {
	try {
		return boost::lexical_cast<T>(value);
	} catch(...) {
		return boost::numeric_cast<T>(boost::lexical_cast<double>(value));
	}
}

template<typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type
type_cast(all_type_variant value) {
	// TODO is lexical_cast always necessary?
	return boost::lexical_cast<T>(value);
}

template<typename T>
typename std::enable_if<std::is_same<T, std::string>::value, T>::type
type_cast(all_type_variant value) {
	return boost::lexical_cast<T>(value);
}

std::string to_string(const all_type_variant& x);

template<class base, template <typename> class impl, class... Args>
std::unique_ptr<base> make_unique_templated(const std::string &type, Args&&... args) {
// TODO can we leave out the base argument?
    base *ret = nullptr;
    hana::for_each(column_types, [&](auto x) {
        if(std::string(hana::first(x)) == type) {
        	typename std::remove_reference<decltype(hana::second(x))>::type prototype;
        	ret = new impl<decltype(prototype)>(args...);
        	return;
        }
    });
    if(DEBUG && !ret) throw std::runtime_error("unknown type " + type);
    return std::unique_ptr<base>(ret);
}

template<class base, template <typename> class impl, class... Args>
std::shared_ptr<base> make_shared_templated(const std::string &type, Args&&... args) {
    return std::move(make_unique_templated<base, impl>(type, args...));
}

}