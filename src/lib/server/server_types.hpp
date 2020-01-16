#pragma once

#include <boost/asio.hpp>

namespace opossum {

using Socket = boost::asio::ip::tcp::socket;

enum class HasNullTerminator : bool { Yes = true, No = false };

enum class SendExecutionInfo : bool { Yes = true, No = false };

}  // namespace opossum
