#pragma once

#include "boost/variant.hpp"

namespace opossum {

/**
 * Taken from https://stackoverflow.com/a/7868427
 */

template <typename ReturnType, typename... Lambdas>
struct lambda_visitor : public boost::static_visitor<ReturnType>, public Lambdas... {
  lambda_visitor(Lambdas... lambdas) : Lambdas(lambdas)... {}
};

template <typename ReturnType, typename... Lambdas>
lambda_visitor<ReturnType, Lambdas...> make_lambda_visitor(Lambdas... lambdas) {
  return { lambdas... };
}

template <typename ReturnType, typename Variant, typename... Lambdas>
ReturnType apply_lambda_visitor(const Variant& variant, Lambdas... lambdas) {
  return boost::apply_visitor(make_lambda_visitor<ReturnType>(lambdas...), variant);
}

}