#pragma once

#include <exception>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace opossum {

/*
 * Hyrise specific exception used to handle errors related to wrong user input.
 * The console will catch this exception when parsing a sql string.
 * Also thrown by the macro AssertInput(expr, msg) to easily check user input related constraints.
 */
class InvalidInputException : public std::runtime_error {
 public:
  explicit InvalidInputException(const std::string& what_arg) : std::runtime_error(what_arg) {}
};

}  // namespace opossum
