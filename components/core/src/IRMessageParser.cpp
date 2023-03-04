//
// Created by haiqixu on 1/30/2022.
//

#include "IRMessageParser.hpp"
#include "../submodules/json/single_include/nlohmann/json.hpp"
#include <iostream>
#include <boost/lexical_cast.hpp>
#include "ffi/encoding_methods.hpp"
#include "ffi/ir_stream/protocol_constants.hpp"
#include "ffi/ir_stream/byteswap.hpp"

constexpr int VAR_STR_LEN_UNSIGNED_BYTE = 0x11;
constexpr int VAR_STR_LEN_UNSIGNED_SHORT = 0x12;
constexpr int VAR_STR_LEN_SIGNED_INT = 0x13;

bool IRMessageParser::is_ir_encoded(const char* buf, bool& is_compacted) {
    bool is_ir_encoded = false;
    if(0 == memcmp(ffi::ir_stream::cProtocol::EightByteEncodingMagicNumber,
                   buf, ffi::ir_stream::cProtocol::MagicNumberLength)) {
        is_compacted = false;
        is_ir_encoded = true;
    } else if (0 == memcmp(ffi::ir_stream::cProtocol::FourByteEncodingMagicNumber,
                           buf, ffi::ir_stream::cProtocol::MagicNumberLength)) {
        is_compacted = true;
        is_ir_encoded = true;
    }
    return is_ir_encoded;
}

template <typename integer_t>
static void read_data_big_endian (ReaderInterface &reader, integer_t& data) {
    integer_t value_small_endian;
    static_assert(sizeof(integer_t) == 1 || sizeof(integer_t) == 2 || sizeof(integer_t) == 4 || sizeof(integer_t) == 8);
    size_t read_size = 0;
    auto error_code = reader.try_read((char*)&value_small_endian, sizeof(integer_t), read_size);
    if (ErrorCode_Success != error_code || read_size != sizeof(integer_t)) {
        SPDLOG_ERROR("Failed to read {} bytes from reader", sizeof(integer_t));
        throw;
    }
    if constexpr (sizeof(integer_t) == 1) {
        data = value_small_endian;
    } else if constexpr (sizeof(integer_t) == 2) {
        data = bswap_16(value_small_endian);
    } else if constexpr (sizeof(integer_t) == 4) {
        data = bswap_32(value_small_endian);
    } else if constexpr (sizeof(integer_t) == 8) {
        data = bswap_64(value_small_endian);
    }
}

static size_t get_logtype_length (ReaderInterface &reader, uint8_t tag_byte) {
    if(tag_byte == ffi::ir_stream::cProtocol::Payload::LogtypeStrLenUByte) {
        uint8_t length;
        read_data_big_endian(reader, length);
        return length;
    } else if (tag_byte == ffi::ir_stream::cProtocol::Payload::LogtypeStrLenUShort) {
        uint16_t length;
        read_data_big_endian(reader, length);
        return length;
    } else if (tag_byte == ffi::ir_stream::cProtocol::Payload::LogtypeStrLenInt) {
        uint32_t length;
        read_data_big_endian(reader, length);
        return length;
    } else {
        SPDLOG_ERROR("Unexpected tag byte {}\n", tag_byte);
        throw;
    }
}



uint8_t IRMessageParser::read_byte (ReaderInterface &reader) {
    uint8_t value = 0;
    size_t num_bytes_to_read = 1;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read byte");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    return value;
}
uint16_t IRMessageParser::read_short (ReaderInterface &reader) {
    uint16_t value;
    size_t num_bytes_to_read = 2;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read short");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    return __builtin_bswap16 (value);
}
uint32_t IRMessageParser::read_unsigned (ReaderInterface &reader) {
    uint32_t value;
    size_t num_bytes_to_read = 4;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read int");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    return __builtin_bswap32 (value);
}
uint64_t IRMessageParser::read_long (ReaderInterface &reader) {
    uint64_t value;
    size_t num_bytes_to_read = 8;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read long");
    }
    return  __builtin_bswap64 (value);
}

static bool is_std_variable_tag(uint8_t tag) {

    if(tag == ffi::ir_stream::cProtocol::Payload::VarEightByteEncoding ||
       tag == ffi::ir_stream::cProtocol::Payload::VarStrLenUByte ||
       tag == ffi::ir_stream::cProtocol::Payload::VarStrLenUShort ||
       tag == ffi::ir_stream::cProtocol::Payload::VarStrLenInt) {
        return true;
    }
    return false;
}

static bool is_compact_variable_tag(uint8_t tag) {

    if(tag == ffi::ir_stream::cProtocol::Payload::VarFourByteEncoding ||
       tag == ffi::ir_stream::cProtocol::Payload::VarStrLenUByte ||
       tag == ffi::ir_stream::cProtocol::Payload::VarStrLenUShort ||
       tag == ffi::ir_stream::cProtocol::Payload::VarStrLenInt) {
        return true;
    }
    return false;
}

bool is_place_holder(char val) {
    if(val == enum_to_underlying_type(ffi::VariablePlaceholder::Integer) ||
       val == enum_to_underlying_type(ffi::VariablePlaceholder::Dictionary) ||
       val == enum_to_underlying_type(ffi::VariablePlaceholder::Float)) {
        return true;
    }
    return false;
}

void IRMessageParser::parse_dictionary_var(ReaderInterface &reader, ParsedIRMessage &message, uint8_t tag_byte) {

    int length;
    if (tag_byte == ffi::ir_stream::cProtocol::Payload::VarStrLenUByte) {
        length = read_byte(reader);
    } else if (tag_byte == ffi::ir_stream::cProtocol::Payload::VarStrLenUShort) {
        length = read_short(reader);
    } else if (tag_byte == ffi::ir_stream::cProtocol::Payload::VarStrLenInt){
        length = read_unsigned(reader);
    } else {
        SPDLOG_ERROR("Unexpected tag byte");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    size_t read_length;
    std::vector<char> variable_buffer_vec(length);
    auto error_code = reader.try_read(variable_buffer_vec.data(), length, read_length);
    if(read_length != length) {
        SPDLOG_ERROR("Failed to read exact length");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    std::string var_str(variable_buffer_vec.data(), length);
    message.append_dict_vars(var_str);
}

void IRMessageParser::parse_log_type(ReaderInterface &reader, ParsedIRMessage &message, uint8_t tag_byte) {

    size_t log_length = get_logtype_length(reader, tag_byte);
    std::vector<char> buffer(log_length);
    size_t read_length;
    auto error_code = reader.try_read(buffer.data(), log_length, read_length);
    if (ErrorCode_Success != error_code || read_length != log_length) {
        SPDLOG_ERROR("Failed to read logtype");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    std::string log_type(buffer.data(), log_length);

    // pre-parse the logtype to find all placeholders
    for(size_t str_pos = 0; str_pos < log_length; str_pos++) {
        auto val = log_type.at(str_pos);
        if(is_place_holder(val)) {
            message.add_placeholder(str_pos);
        }
    }

    message.set_log_type(log_type);
}

bool IRMessageParser::parse_next_compact_message(ReaderInterface &reader, ParsedIRMessage &message) {
    message.clear_except_ts_patt();

    uint8_t tag_byte;
    tag_byte = read_byte(reader);
    if(tag_byte == ffi::ir_stream::cProtocol::Eof) {
        return false;
    }

    while(is_compact_variable_tag(tag_byte)) {

        if (tag_byte == ffi::ir_stream::cProtocol::Payload::VarFourByteEncoding) {
            encoded_variable_t var_compact = read_unsigned(reader);
            message.append_encoded_vars(var_compact);
        }
        else {
            // else case, variables are basically strings
            parse_dictionary_var(reader, message, tag_byte);
        }
        tag_byte = read_byte(reader);
    }

    parse_log_type(reader, message, tag_byte);

    tag_byte = read_byte(reader);

    // handle timestamp
    epochtime_t timestamp_delta_value = 0;
    if(tag_byte == ffi::ir_stream::cProtocol::Payload::TimestampDeltaByte) {
        timestamp_delta_value = read_byte(reader);
    } else if (tag_byte == ffi::ir_stream::cProtocol::Payload::TimestampDeltaShort) {
        timestamp_delta_value = read_short(reader);
    } else if (tag_byte == ffi::ir_stream::cProtocol::Payload::TimestampDeltaInt) {
        timestamp_delta_value = read_unsigned(reader);
    } else {
        SPDLOG_ERROR("Unexpected timestamp tag {}", tag_byte);
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    epochtime_t timestamp = timestamp_delta_value + m_last_timestamp;
    m_last_timestamp = timestamp;
    message.set_time(timestamp);
    return true;
}


bool IRMessageParser::parse_next_std_message(ReaderInterface &reader, ParsedIRMessage &message) {

    message.clear_except_ts_patt();

    uint8_t tag_byte;
    tag_byte = read_byte(reader);
    if(tag_byte == ffi::ir_stream::cProtocol::Eof) {
        return false;
    }

    while(is_std_variable_tag(tag_byte)) {
        if (tag_byte == ffi::ir_stream::cProtocol::Payload::VarEightByteEncoding) {
            // could be an issue?
            encoded_variable_t var_standard = read_long(reader);
            message.append_encoded_vars(var_standard);
        }
        // else case, variables are basically strings
        else {
            parse_dictionary_var(reader, message, tag_byte);
        }
        tag_byte = read_byte(reader);
    }

    parse_log_type(reader, message, tag_byte);

    // now parse timestamp
    tag_byte = read_byte(reader);

    if(tag_byte != ffi::ir_stream::cProtocol::Payload::TimestampVal) {
        SPDLOG_ERROR("Unexpected timestamp tag {}", tag_byte);
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }

    epochtime_t timestamp = read_long(reader);
    message.set_time(timestamp);
    return true;
}

bool IRMessageParser::parse_next_message (ReaderInterface& reader, ParsedIRMessage& message) {
    if(m_compact_encoding) {
        return parse_next_compact_message(reader, message);
    } else {
        return parse_next_std_message(reader, message);
    }
}

bool IRMessageParser::parse_metadata(ReaderInterface &reader, ParsedIRMessage &message, bool is_compact_encoding) {

    uint8_t encoding_tag;
    encoding_tag = read_byte(reader);
    // Check the metadata byte
    if(encoding_tag != ffi::ir_stream::cProtocol::Metadata::EncodingJson) {
        SPDLOG_ERROR("Invalid Encoding Tag {}", encoding_tag);
        return false;
    }

    uint8_t length_tag = read_byte(reader);

    unsigned int metadata_length;
    switch(length_tag) {
        case ffi::ir_stream::cProtocol::Metadata::LengthUByte:
            metadata_length = read_byte(reader);
            break;
        case ffi::ir_stream::cProtocol::Metadata::LengthUShort:
            metadata_length = read_short(reader);
            break;
        default:
            SPDLOG_ERROR("Invalid Length Tag {}", length_tag);
            return false;
    }

    size_t read_length;
    std::vector<char> buffer_vec(metadata_length);
    auto error_code = reader.try_read(buffer_vec.data(), metadata_length, read_length);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read metadata");
        return false;
    }
    std::string buffer_str(buffer_vec.data(), metadata_length);
    auto metadata_json = nlohmann::json::parse(buffer_str);
    // TODO: LET IT USE THE TRUE TIMESTAMP FORMAT
    // std::string time_stamp_string = j3.at("TIMESTAMP_PATTERN");
    std::string time_stamp_string = "%y/%m/%d %H:%M:%S";
    m_timezone = metadata_json.at(ffi::ir_stream::cProtocol::Metadata::TimeZoneIdKey);
    m_version = metadata_json.at(ffi::ir_stream::cProtocol::Metadata::VersionKey);
    if (m_version != ffi::ir_stream::cProtocol::Metadata::VersionValue) {
        SPDLOG_ERROR("Deprecated version: {}", m_version);
    }
    if(is_compact_encoding) {
        std::string reference_timestamp =
                metadata_json.at(ffi::ir_stream::cProtocol::Metadata::ReferenceTimestampKey);
        m_last_timestamp = boost::lexical_cast<epochtime_t>(reference_timestamp);
        time_stamp_string = "%Y-%m-%dT%H:%M:%S.%3Z";
    }
    message.set_ts_pattern(0, time_stamp_string);

    m_compact_encoding = is_compact_encoding;
    message.set_compact(is_compact_encoding);
    return true;
}