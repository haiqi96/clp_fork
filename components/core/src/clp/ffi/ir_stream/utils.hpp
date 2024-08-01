#ifndef CLP_FFI_IR_STREAM_UTILS_HPP
#define CLP_FFI_IR_STREAM_UTILS_HPP

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <json/single_include/nlohmann/json.hpp>

#include "../../ir/types.hpp"
#include "byteswap.hpp"
#include "encoding_methods.hpp"
#include "protocol_constants.hpp"

namespace clp::ffi::ir_stream {
/**
 * Serializes the given metadata into the IR stream.
 * @param metadata
 * @param output_buf
 * @return Whether serialization succeeded.
 */
[[nodiscard]] auto
serialize_metadata(nlohmann::json& metadata, std::vector<int8_t>& output_buf) -> bool;

/**
 * Serializes the given integer into the IR stream.
 * @tparam integer_t
 * @param value
 * @param output_buf
 */
template <typename integer_t>
auto serialize_int(integer_t value, std::vector<int8_t>& output_buf) -> void;

/**
 * Serializes a string using CLP's encoding for unstructured text.
 * @tparam encoded_variable_t
 * @param str
 * @param logtype Returns the corresponding logtype.
 * @param output_buf
 * @return Whether serialization succeeded.
 */
template <typename encoded_variable_t>
[[nodiscard]] auto serialize_clp_string(
        std::string_view str,
        std::string& logtype,
        std::vector<int8_t>& output_buf
) -> bool;

/**
 * Serializes a string.
 * @param str
 * @param output_buf
 * @return Whether serialization succeeded.
 */
[[nodiscard]] auto serialize_string(std::string_view str, std::vector<int8_t>& output_buf) -> bool;

template <typename integer_t>
auto serialize_int(integer_t value, std::vector<int8_t>& output_buf) -> void {
    integer_t value_big_endian{};
    static_assert(sizeof(integer_t) == 2 || sizeof(integer_t) == 4 || sizeof(integer_t) == 8);
    if constexpr (sizeof(value) == 2) {
        value_big_endian = bswap_16(value);
    } else if constexpr (sizeof(value) == 4) {
        value_big_endian = bswap_32(value);
    } else if constexpr (sizeof(value) == 8) {
        value_big_endian = bswap_64(value);
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    std::span<int8_t> const data_view{reinterpret_cast<int8_t*>(&value_big_endian), sizeof(value)};
    output_buf.insert(output_buf.end(), data_view.begin(), data_view.end());
}

template <typename encoded_variable_t>
[[nodiscard]] auto serialize_clp_string(
        std::string_view str,
        std::string& logtype,
        std::vector<int8_t>& output_buf
) -> bool {
    static_assert(
            (std::is_same_v<encoded_variable_t, clp::ir::eight_byte_encoded_variable_t>
             || std::is_same_v<encoded_variable_t, clp::ir::four_byte_encoded_variable_t>)
    );
    bool succeeded{};
    if constexpr (std::is_same_v<encoded_variable_t, clp::ir::four_byte_encoded_variable_t>) {
        output_buf.push_back(cProtocol::Payload::ValueFourByteEncodingClpStr);
        succeeded = four_byte_encoding::serialize_message(str, logtype, output_buf);
    } else {
        output_buf.push_back(cProtocol::Payload::ValueEightByteEncodingClpStr);
        succeeded = eight_byte_encoding::serialize_message(str, logtype, output_buf);
    }
    return succeeded;
}
}  // namespace clp::ffi::ir_stream
#endif
