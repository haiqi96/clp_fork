#include "IrMessageParser.hpp"

#include "../../../submodules/json/single_include/nlohmann/json.hpp"
#include "BufferReader.hpp"
#include "EncodedVariableInterpreter.hpp"
#include "ffi/encoding_methods.hpp"
#include "ffi/ir_stream/protocol_constants.hpp"
#include "spdlog/spdlog.h"

using ffi::cVariablePlaceholderEscapeCharacter;
using ffi::eight_byte_encoded_variable_t;
using ffi::four_byte_encoded_variable_t;
using ffi::ir_stream::cProtocol::MagicNumberLength;
using ffi::ir_stream::IRErrorCode;
using ffi::VariablePlaceholder;
using std::string;
using std::vector;

/**
 * Constructs the class by setting the internal reader, parsing the metadata
 * and initializing variable based on the metadata
 * @param reader
 * @throw OperationFailed if the reader doesn't contain IR encoded data,
 * or IR data that can't be properly decoded
 */
IrMessageParser::IrMessageParser(ReaderInterface& reader) : m_reader(reader) {
    if (ffi::ir_stream::IRErrorCode_Success
        != ffi::ir_stream::get_encoding_type(reader, m_is_four_bytes_encoded)) {
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }

    string json_metadata;
    if (false == decode_json_preamble(json_metadata)) {
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }

    string const mocked_ts_pattern = "%Y-%m-%dT%H:%M:%S.%3";
    try {
        auto metadata_json = nlohmann::json::parse(json_metadata);
        string version = metadata_json.at(ffi::ir_stream::cProtocol::Metadata::VersionKey);
        if (version != ffi::ir_stream::cProtocol::Metadata::VersionValue) {
            SPDLOG_ERROR("Input IR has unsupported version {}", version);
            throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
        }

        // For now, use a fixed timestamp pattern
        m_ts_pattern = TimestampPattern(0, mocked_ts_pattern);

        if (m_is_four_bytes_encoded) {
            m_reference_timestamp = std::stoll(
                    metadata_json.at(ffi::ir_stream::cProtocol::Metadata::ReferenceTimestampKey)
                            .get<string>()
            );
            m_msg.set_ts(m_reference_timestamp);
        }

    } catch (nlohmann::json::parse_error const& e) {
        SPDLOG_ERROR("Failed to parse json metadata from reader");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    m_msg.set_ts_pattern(&m_ts_pattern);
}

auto IrMessageParser::parse_next_encoded_message() -> bool {
    if (m_is_four_bytes_encoded) {
        return parse_next_four_bytes_message();
    }
    return parse_next_eight_bytes_message();
}

auto IrMessageParser::is_ir_encoded(size_t sequence_length, char const* data) -> bool {
    if (sequence_length < MagicNumberLength) {
        return false;
    }
    bool is_four_bytes_encoded{false};
    BufferReader encoding_data(data, MagicNumberLength);
    if (ffi::ir_stream::IRErrorCode_Success
        != ffi::ir_stream::get_encoding_type(encoding_data, is_four_bytes_encoded))
    {
        return false;
    }
    return true;
}

auto IrMessageParser::parse_next_eight_bytes_message() -> bool {
    m_msg.clear();

    epochtime_t ts{0};
    vector<eight_byte_encoded_variable_t> encoded_vars;
    vector<string> dict_vars;
    string logtype;

    auto error_code
            = ffi::ir_stream::generic_parse_tokens(m_reader, logtype, encoded_vars, dict_vars, ts);

    if (IRErrorCode::IRErrorCode_Success != error_code) {
        if (IRErrorCode::IRErrorCode_Eof != error_code) {
            SPDLOG_ERROR("Corrupted IR, error code: {}", error_code);
        }
        return false;
    }

    auto constant_handler = [this](std::string const& value, size_t begin_pos, size_t length) {
        m_msg.append_to_logtype(value, begin_pos, length);
    };

    auto constant_remainder_handler = [this](std::string const& value, size_t begin_pos) {
        auto const remaining_size = value.length() - begin_pos;
        m_msg.append_to_logtype(value, begin_pos, remaining_size);
    };

    auto encoded_int_handler = [this](eight_byte_encoded_variable_t value) {
        auto decoded_int = ffi::decode_integer_var(value);
        m_msg.add_encoded_integer(value, decoded_int.length());
    };

    auto encoded_float_handler = [this](eight_byte_encoded_variable_t encoded_float) {
        auto decoded_float = ffi::decode_float_var(encoded_float);
        m_msg.add_encoded_float(encoded_float, decoded_float.size());
    };

    auto dict_var_handler = [this](string const& dict_var) { m_msg.add_dictionary_var(dict_var); };

    // handle timestamp
    m_msg.set_ts(ts);
    try {
        ffi::ir_stream::generic_decode_message(
                logtype,
                encoded_vars,
                dict_vars,
                constant_handler,
                constant_remainder_handler,
                encoded_int_handler,
                encoded_float_handler,
                dict_var_handler
        );
    } catch (ffi::ir_stream::DecodingException& e) {
        SPDLOG_ERROR("Decoding failed with exception {}", e.what());
        return false;
    }

    return true;
}

auto IrMessageParser::parse_next_four_bytes_message() -> bool {
    m_msg.clear();

    epochtime_t ts{0};
    vector<four_byte_encoded_variable_t> encoded_vars;
    vector<string> dict_vars;
    string logtype;

    auto error_code
            = ffi::ir_stream::generic_parse_tokens(m_reader, logtype, encoded_vars, dict_vars, ts);

    if (IRErrorCode::IRErrorCode_Success != error_code) {
        if (IRErrorCode::IRErrorCode_Eof != error_code) {
            SPDLOG_ERROR("Corrupted IR, error code: {}", error_code);
        }
        return false;
    }

    auto constant_handler = [this](std::string const& value, size_t begin_pos, size_t length) {
        m_msg.append_to_logtype(value, begin_pos, length);
    };

    auto constant_remainder_handler = [this](std::string const& value, size_t begin_pos) {
        auto const remaining_size = value.length() - begin_pos;
        m_msg.append_to_logtype(value, begin_pos, remaining_size);
    };

    auto encoded_int_handler = [this](four_byte_encoded_variable_t value) {
        // assume that we need the actual size
        auto decoded_int = ffi::decode_integer_var(value);
        m_msg.add_encoded_integer(value, decoded_int.length());
    };

    auto encoded_float_handler = [this](four_byte_encoded_variable_t encoded_float) {
        const auto original_size_in_bytes = ffi::decode_float_var(encoded_float).size();
        eight_byte_encoded_variable_t converted_float{0};
        EncodedVariableInterpreter::convert_four_bytes_float_to_eight_byte(
                encoded_float,
                converted_float
        );
        m_msg.add_encoded_float(converted_float, original_size_in_bytes);
    };

    auto dict_var_handler = [this](string const& dict_var) {
        encoded_variable_t converted_var{0};
        if (EncodedVariableInterpreter::convert_string_to_representable_integer_var(
                    dict_var,
                    converted_var
            ))
        {
            m_msg.add_encoded_integer(converted_var, dict_var.size());
        } else if (EncodedVariableInterpreter::convert_string_to_representable_float_var(
                           dict_var,
                           converted_var
                   ))
        {
            m_msg.add_encoded_float(converted_var, dict_var.size());
        } else {
            m_msg.add_dictionary_var(dict_var);
        }
    };

    // handle timestamp
    m_reference_timestamp += ts;
    m_msg.set_ts(m_reference_timestamp);
    try {
        ffi::ir_stream::generic_decode_message(
                logtype,
                encoded_vars,
                dict_vars,
                constant_handler,
                constant_remainder_handler,
                encoded_int_handler,
                encoded_float_handler,
                dict_var_handler
        );
    } catch (ffi::ir_stream::DecodingException& e) {
        SPDLOG_ERROR("Decoding failed with exception {}", e.what());
        return false;
    }

    return true;
}

auto IrMessageParser::decode_json_preamble(std::string& json_metadata) -> bool {
    // Decode and parse metadata
    ffi::ir_stream::encoded_tag_t metadata_type{0};
    std::vector<int8_t> metadata_vec;

    if (ffi::ir_stream::IRErrorCode_Success
        != ffi::ir_stream::decode_preamble(m_reader, metadata_type, metadata_vec))
    {
        SPDLOG_ERROR("Failed to parse metadata");
        return false;
    }

    if (ffi::ir_stream::cProtocol::Metadata::EncodingJson != metadata_type) {
        SPDLOG_ERROR("Unexpected metadata type {}", metadata_type);
        return false;
    }

    json_metadata.assign(
            size_checked_pointer_cast<char const>(metadata_vec.data()),
            metadata_vec.size()
    );

    return true;
}
