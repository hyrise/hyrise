# Over time, Nix will update their package and newer versions might be available. If that time comes, you might want to update to the newest version.
#
# How to update this:
#   1. Find the new desired commit hash on github.com/nixos/nixpkgs and copy it. Most of the time, this will be the latest commit on the master branch. However, you can also choose to use different branches that are most stable.
#   2. Put the commit hash as first argument to the fetchCommit function call. Now, the tarball fetched for that commit does not anymore match the sha256 hash given in the function.
#   3. Run `nix-shell --pure` in this directory. You will likely run into an error. The error will contain a sha256 hash that fetchTarball requires.
#   4. Take the commit hash from the error and copy it in the sha256 field.

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
