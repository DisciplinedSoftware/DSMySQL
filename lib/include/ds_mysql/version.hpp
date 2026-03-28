#pragma once

#include <cstdint>
#include <string_view>

#if __has_include("ds_mysql/version_generated.hpp")

#include "ds_mysql/version_generated.hpp"

#else

namespace ds_mysql {

/// Compile-time version information for DSMySQL.
///
/// This checked-in fallback is used only when no build-system generated
/// `version_generated.hpp` is present (e.g. raw-header, non-CMake usage).
struct version {
    static constexpr std::uint32_t major = 3;
    static constexpr std::uint32_t minor = 1;
    static constexpr std::uint32_t patch = 0;

    /// Packed integer: major * 10000 + minor * 100 + patch.
    static constexpr std::uint32_t value = major * 10'000u + minor * 100u + patch;

    /// Canonical fallback string for non-generated builds.
    static constexpr std::string_view string = "3.1.0";
};

}  // namespace ds_mysql

#endif