# Nix Packages

[Nix] is a package manager for Linux and macOS. It targets reproducible builds by providing fixed versions of packages and their dependencies.

## Derivations

Nix itself does not hold any executables. Instead, it offers a script that tells Nix how to acquire the executable by, for example, downloading a release version from GitHub. This script is called a derivation. Derivations are written in the [Nix functional language].

## Pinning the Nix Package Manager

The Nix package manager should be pinned to a specific version. This is useful when the user wants to ensure that the package manager does not change its behavior over time.

To do so, the [`default.nix`] file defines a Git hash pointing to a specific version of the package manager's GitHub repository. The hash is used to determine the exact version of the package manager and its derivations. It should be updated regularly.

[Nix]: https://nixos.org/
[Nix functional language]: https://nix.dev/tutorials/nix-language
[Nix package manager]: https://github.com/NixOS/nixpkgs
[Overlays]: https://nixos.wiki/wiki/Overlays
[`default.nix`]: ./default.nix
