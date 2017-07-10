#pragma once

/**
 * Disable some GCC warnings when including proto generated files
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include "generated/opossum.pb.h"
#pragma GCC diagnostic pop
