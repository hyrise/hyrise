#pragma once

#include <exception>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace opossum {

// Server-specific exception used to handle errors when reading data from or writing data to the network socket.
class ClientDisconnectException : public std::runtime_error {
 public:
  explicit ClientDisconnectException(const std::string& what_arg) : std::runtime_error(what_arg) {}
};

}  // namespace opossum
