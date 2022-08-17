#pragma once

#include <boost/asio.hpp>

namespace hyrise {

using Socket = boost::asio::ip::tcp::socket;

enum class HasNullTerminator : bool { Yes = true, No = false };

enum class SendExecutionInfo : bool { Yes = true, No = false };

}  // namespace hyrise
