{}:

let
  hashes = import ./hashes.nix;
  fetchCommit = commit: fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${commit}.tar.gz";
    sha256 = "0043xxvs81chfbn01irfj81pj8j9nzwnv4srmaqy38y3bzacv6ra";
  };
in

import (fetchCommit "3563ebc663d7b75c7781085229ee13c17ea93d12") {
  overlays = [];
}
