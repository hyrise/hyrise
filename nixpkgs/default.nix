# How to update this?
#
# 1. Find the new desired commit hash on github.com/nixos/nixpkgs.
# 2. Run `nix-shell --pure` in this directory. You will likely run into an error.
#    The error will contain a sha256 hash that fetchTarball requires.
# 3. Take the commit hash from the error and copy it here. -> Done.

{}:

let
  fetchCommit = commit: fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${commit}.tar.gz";
    sha256 = "0043xxvs81chfbn01irfj81pj8j9nzwnv4srmaqy38y3bzacv6ra";
  };
in

import (fetchCommit "3563ebc663d7b75c7781085229ee13c17ea93d12") {
  overlays = [];
}
