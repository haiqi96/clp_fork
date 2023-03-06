//
// Created by haiqixu on 1/30/2022.
//

#include "ParsedIRMessage.hpp"

// spdlog
#include <spdlog/spdlog.h>
#include "ffi/encoding_methods.hpp"
#include "EncodedVariableInterpreter.hpp"

using ffi::VariablePlaceholder;

void ParsedIRMessage::set_ts_pattern(uint8_t num_spaces_before_ts, const std::string &format) {
    if(m_ts_patt != nullptr) {
        SPDLOG_ERROR("Unexpected not nullptr");
    }
    m_ts_patt = std::make_unique<TimestampPattern>(num_spaces_before_ts, format);
}

// TODO: this might be an issue. because we first clear it and then initialize
// may be use a variable to check if this is initialized
void ParsedIRMessage::clear() {
    if(m_ts_patt != nullptr) {
        m_ts_patt.reset();
    }
    m_dictionary_vars.clear();
    m_encoded_vars.clear();
    m_placeholder_pos.clear();
}

void ParsedIRMessage::recover_message(std::string& message) {
    message.clear();
    size_t begin_pos = 0;
    size_t encoded_var_ix = 0;
    size_t dictionary_var_ix = 0;
    for(const auto& pos : m_placeholder_pos) {
        message.append(m_log_type, begin_pos, pos-begin_pos);
        // assume that logtype all ends with \n so pos will never be the end of string
        begin_pos = pos + 1;
        auto placeholder = m_log_type.at(pos);
        if(placeholder == enum_to_underlying_type(VariablePlaceholder::Integer)) {
            message += std::to_string(m_encoded_vars.at(encoded_var_ix++));
        } else if (placeholder == enum_to_underlying_type(VariablePlaceholder::Float)) {
            auto encoded_var = m_encoded_vars.at(encoded_var_ix++);
            std::string decoded_str;
            if(m_is_compact) {
                EncodedVariableInterpreter::convert_compact_encoded_double_to_string(encoded_var, decoded_str);
            } else {
                EncodedVariableInterpreter::convert_encoded_double_to_string(encoded_var, decoded_str);
            }
            message.append(decoded_str);
        } else if (placeholder == enum_to_underlying_type(VariablePlaceholder::Dictionary)) {
            message.append(m_dictionary_vars.at(dictionary_var_ix++));
        } else {
            SPDLOG_ERROR("unexpected place holder");
            throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
        }
    }
    message.append(m_log_type, begin_pos);
    m_ts_patt->insert_formatted_timestamp(m_ts, message);
}

void ParsedIRMessage::clear_except_ts_patt() {
    m_dictionary_vars.clear();
    m_encoded_vars.clear();
    m_placeholder_pos.clear();
}