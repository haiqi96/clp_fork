//
// Created by haiqixu on 1/30/2022.
//

#include "EncodedParsedMessage.hpp"

// spdlog
#include <spdlog/spdlog.h>
#include "ffi/encoding_methods.hpp"

void EncodedParsedMessage::set_ts_pattern(uint8_t num_spaces_before_ts, const std::string &format) {
    if(m_ts_patt != nullptr) {
        SPDLOG_ERROR("Unexpected not nullptr");
    }
    m_ts_patt = std::make_unique<TimestampPattern>(num_spaces_before_ts, format);
}

// TODO: this might be an issue. because we first clear it and then initialize
// may be use a variable to check if this is initialized
void EncodedParsedMessage::clear() {
    if(m_ts_patt != nullptr) {
        m_ts_patt.reset();
    }
    m_unencoded_vars.clear();
    m_encoded_vars.clear();
    m_order.clear();
}

static void convert_compact_encoded_double_to_string (encoded_variable_t encoded_var, std::string& value) {
    uint64_t encoded_double;
    static_assert(sizeof(encoded_double) == sizeof(encoded_var), "sizeof(encoded_double) != sizeof(encoded_var)");
    // NOTE: We use memcpy rather than reinterpret_cast to avoid violating strict aliasing; a smart compiler should optimize it to a register move
    std::memcpy(&encoded_double, &encoded_var, sizeof(encoded_var));

    assert((encoded_double & 0xFFFFFFFF00000000) == 0);

    // Decode according to the format described in EncodedVariableInterpreter::convert_string_to_representable_double_var
    uint8_t decimal_pos = (encoded_double & 0x07) + 1;
    encoded_double >>= 3;
    uint8_t num_digits = (encoded_double & 0x07) + 1;
    encoded_double >>= 3;
    uint64_t digits = encoded_double & 0x1FFFFFF;
    encoded_double >>= 25;
    bool is_negative = encoded_double > 0;

    size_t value_length = num_digits + 1 + is_negative;
    value.resize(value_length);
    size_t num_chars_to_process = value_length;

    // Add sign
    if (is_negative) {
        value[0] = '-';
        --num_chars_to_process;
    }

    // Decode until the decimal or the non-zero digits are exhausted
    size_t pos = value_length - 1;
    for (; pos > (value_length - 1 - decimal_pos) && digits > 0; --pos) {
        value[pos] = (char)('0' + (digits % 10));
        digits /= 10;
        --num_chars_to_process;
    }

    if (digits > 0) {
        // Skip decimal since it's added at the end
        --pos;
        --num_chars_to_process;

        while (digits > 0) {
            value[pos--] = (char)('0' + (digits % 10));
            digits /= 10;
            --num_chars_to_process;
        }
    }

    // Add remaining zeros
    for (; num_chars_to_process > 0; --num_chars_to_process) {
        value[pos--] = '0';
    }

    // Add decimal
    value[value_length - 1 - decimal_pos] = '.';
}

std::string EncodedParsedMessage::recover_message() {
    std::string return_string;
    size_t original_length = m_log_strings.size();
    constexpr int id_delimi = 18;
    constexpr int inter_delimi = 17;
    constexpr int double_delimi = 19;
    size_t unencoded_var_ix = 0;
    size_t encoded_var_ix = 0;
    for(size_t ix = 0; ix < original_length; ix++) {
        char current_char = m_log_strings.at(ix);
        if(current_char != (char)double_delimi &&
           current_char != (char)inter_delimi &&
           current_char != (char)id_delimi)
        {
            return_string += current_char;
        } else if (current_char == id_delimi) {
            return_string += m_unencoded_vars[unencoded_var_ix];
            unencoded_var_ix++;
        } else if (current_char == inter_delimi) {
            return_string += std::to_string(m_encoded_vars[encoded_var_ix]);
            encoded_var_ix++;
        } else {
            auto encoded_var = m_encoded_vars[encoded_var_ix];
            std::string decoded_str;
            convert_compact_encoded_double_to_string(encoded_var, decoded_str);
            return_string += decoded_str;
            encoded_var_ix++;
        }
    }
    m_ts_patt->insert_formatted_timestamp(m_ts, return_string);
    return return_string;
}

void EncodedParsedMessage::clear_except_ts_patt() {
    m_unencoded_vars.clear();
    m_encoded_vars.clear();
    m_order.clear();
}