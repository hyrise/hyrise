#pragma once

// We need this file to store the current GIT hash and make it available to our binaries.

// The SHA1 for the HEAD of the repo.
#define GIT_HEAD_SHA1 "210f815bacc2cd2caad1bc3a01da6c2cea89b4e4"

// Whether or not there were uncommitted changes present.
#define GIT_IS_DIRTY true
