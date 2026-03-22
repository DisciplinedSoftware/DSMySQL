#pragma once

#include <cstdint>
#include <string_view>

namespace ds_mysql {

/// Compile-time version information for DSMySQL.
///
/// This checked-in header is a fallback for non-CMake consumption.
/// In CMake builds, `version.hpp.in` is configured into the build tree and
/// should be preferred so `project(VERSION ...)` stays authoritative.
struct version {
    static constexpr std::uint32_t major  = 1;
    static constexpr std::uint32_t minor  = 0;
    static constexpr std::uint32_t patch  = 0;

    /// Packed integer: major * 10000 + minor * 100 + patch.
    static constexpr std::uint32_t value  = major * 10'000u + minor * 100u + patch;

    /// Canonical "major.minor.patch" string.
    static constexpr std::string_view string = "1.0.0";
};

}  // namespace ds_mysql