//
// Created by haiqixu on 1/30/2022.
//

#include "EncodedMessageParser.hpp"
#include "../submodules/json/single_include/nlohmann/json.hpp"
#include <iostream>
#include <boost/lexical_cast.hpp>
#include "ffi/encoding_methods.hpp"

constexpr int JSON_ENCODING = 0x01;

constexpr int VAR_COMPACT_ENCODING = 0x18;
constexpr int VAR_STANDARD_ENCODING = 0x19;
constexpr int VAR_STR_LEN_UNSIGNED_BYTE = 0x11;
constexpr int VAR_STR_LEN_UNSIGNED_SHORT = 0x12;
constexpr int VAR_STR_LEN_SIGNED_INT = 0x13;

unsigned char EncodedMessageParser::read_byte (ReaderInterface &reader) {
    unsigned char value = 0;
    size_t num_bytes_to_read = 1;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read byte");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    return value;
}
unsigned short EncodedMessageParser::read_short (ReaderInterface &reader) {
    unsigned short value;
    size_t num_bytes_to_read = 2;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read short");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    return __builtin_bswap16 (value);
}
unsigned int EncodedMessageParser::read_unsigned (ReaderInterface &reader) {
    unsigned int value;
    size_t num_bytes_to_read = 4;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read int");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    return __builtin_bswap32 (value);
}
unsigned long long EncodedMessageParser::read_long (ReaderInterface &reader) {
    unsigned long long value;
    size_t num_bytes_to_read = 8;
    size_t read_result;
    auto error_code = reader.try_read((char*)&value, num_bytes_to_read, read_result);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read long");
    }
    return  __builtin_bswap64 (value);
}

bool is_std_variable_encoding_type(unsigned char tag) {

    if(tag == VAR_STANDARD_ENCODING ||
       tag == VAR_STR_LEN_UNSIGNED_BYTE ||
       tag == VAR_STR_LEN_UNSIGNED_SHORT ||
       tag == VAR_STR_LEN_SIGNED_INT){
        return true;
    }
    return false;
}

bool is_compact_variable_encoding_type(unsigned char tag) {

    if(tag == VAR_COMPACT_ENCODING ||
       tag == VAR_STR_LEN_UNSIGNED_BYTE ||
       tag == VAR_STR_LEN_UNSIGNED_SHORT ||
       tag == VAR_STR_LEN_SIGNED_INT){
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

void EncodedMessageParser::parse_unencoded_vars(ReaderInterface &reader, EncodedParsedMessage &message, unsigned char tag_byte) {
    // else case, variables are basically strings
    int length;
    if (tag_byte == VAR_STR_LEN_UNSIGNED_BYTE) {
        length = read_byte(reader);
    } else if (tag_byte == VAR_STR_LEN_UNSIGNED_SHORT) {
        length = read_short(reader);
    } else if (tag_byte == VAR_STR_LEN_SIGNED_INT){
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

void EncodedMessageParser::parse_log_type(ReaderInterface &reader, EncodedParsedMessage &message, unsigned char tag_byte) {
    constexpr int LOGTYPE_STR_LEN_UNSIGNED_BYTE = 0x21;
    constexpr int LOGTYPE_STR_LEN_UNSIGNED_SHORT = 0x22;
    constexpr int LOGTYPE_STR_LEN_SIGNED_INT = 0x23;

    if(tag_byte != LOGTYPE_STR_LEN_UNSIGNED_BYTE &&
       tag_byte != LOGTYPE_STR_LEN_UNSIGNED_SHORT &&
       tag_byte != LOGTYPE_STR_LEN_SIGNED_INT) {
        SPDLOG_ERROR("Unexpected log tag");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }

    unsigned int log_length;
    size_t read_length;
    if(tag_byte == LOGTYPE_STR_LEN_UNSIGNED_BYTE) {
        log_length = read_byte(reader);
    } else if (tag_byte == LOGTYPE_STR_LEN_UNSIGNED_SHORT) {
        log_length = read_short(reader);
    } else {
        log_length = read_unsigned(reader);
    }
    std::vector<char> buffer(log_length);
    auto error_code = reader.try_read(buffer.data(), log_length, read_length);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to parse log");
        throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
    }
    std::string log_type(buffer.data(), log_length);

    for(size_t str_pos = 0; str_pos < log_length; str_pos++) {
        auto val = log_type.at(str_pos);
        if(is_place_holder(val)) {
            message.add_placeholder(str_pos);
        }
    }

    message.set_log_type(log_type);
}

bool EncodedMessageParser::parse_next_compact_token(ReaderInterface &reader, EncodedParsedMessage &message) {
    message.clear_except_ts_patt();

    unsigned char tag_byte;
    tag_byte = read_byte(reader);
    constexpr int ENDOFFILE = 0x0;
    if(tag_byte == ENDOFFILE) {
        return false;
    }

    while(is_compact_variable_encoding_type(tag_byte)) {

        // question: how do I extract those variables
        if (tag_byte == VAR_COMPACT_ENCODING) {
            // could be an issue?
            encoded_variable_t var_compact = read_unsigned(reader);
            message.append_encoded_vars(var_compact);
        }
        else {
            // else case, variables are basically strings
            parse_unencoded_vars(reader, message, tag_byte);
        }
        tag_byte = read_byte(reader);
    }

    parse_log_type(reader, message, tag_byte);

    tag_byte = read_byte(reader);

    // 64-bit timestamp as a milliseconds from the Unix epoch
    constexpr int TIMESTAMP_DELTA_SIGNED_BYTE = 0x31;   // Only used in compact encoding
    constexpr int TIMESTAMP_DELTA_SIGNED_SHORT = 0x32;   // Only used in compact encoding
    constexpr int TIMESTAMP_DELTA_SIGNED_INT = 0x33;   // Only used in compact encoding

    // handle timestamp
    epochtime_t timestamp_delta_value = 0;
    if(tag_byte == TIMESTAMP_DELTA_SIGNED_BYTE) {
        timestamp_delta_value = read_byte(reader);
    } else if (tag_byte == TIMESTAMP_DELTA_SIGNED_SHORT) {
        timestamp_delta_value = read_short(reader);
    } else if (tag_byte == TIMESTAMP_DELTA_SIGNED_INT) {
        timestamp_delta_value = read_unsigned(reader);
    } else {
        std::cout << "unexpected timestamp tag\n";
        exit(-1);
    }
    epochtime_t timestamp = timestamp_delta_value + m_last_timestamp;
    //std::cout << "Delta timestamp is " << timestamp_delta_value << ". last timestamp is " << message.get_last_timestamp() << ". final timestamp is " << timestamp << std::endl;
    m_last_timestamp = timestamp;
    //TODO: Remove this date hack. note this is different from standard encoding. Most probably due to winter time.
    message.set_time(timestamp);
    return true;
}


bool EncodedMessageParser::parse_next_std_token(ReaderInterface &reader, EncodedParsedMessage &message) {
    message.clear_except_ts_patt();

    unsigned char tag_byte;
    tag_byte = read_byte(reader);
    constexpr int ENDOFFILE = 0x0;
    if(tag_byte == ENDOFFILE) {
        return false;
    }

    while(is_std_variable_encoding_type(tag_byte)) {

        // question: how do I extract those variables
        if (tag_byte == VAR_STANDARD_ENCODING) {
            // could be an issue?
            encoded_variable_t var_standard = read_long(reader);
            message.append_encoded_vars(var_standard);
        }
        // else case, variables are basically strings
        else {
            parse_unencoded_vars(reader, message, tag_byte);
        }
        tag_byte = read_byte(reader);
    }

    parse_log_type(reader, message, tag_byte);

    tag_byte = read_byte(reader);

    // 64-bit timestamp as a milliseconds from the Unix epoch
    // Note that the range 0x30-0x3F is reserved for timestamp
    constexpr int TIMESTAMP_VAL = 0x30;   // Only used in standard encoding

    if(tag_byte != TIMESTAMP_VAL) {
        std::cout << "unexpected timestamp tag\n";
        exit(-1);
    }

    epochtime_t timestamp = read_long(reader);
    //TODO: Remove this date hack
    message.set_time(timestamp);
    return true;
}

bool EncodedMessageParser::parse_next_token (ReaderInterface& reader, EncodedParsedMessage& message) {
    if(m_compact_encoding) {
        return parse_next_compact_token(reader, message);
    } else {
        return parse_next_std_token(reader, message);
    }
}

bool EncodedMessageParser::parse_metadata(ReaderInterface &reader, EncodedParsedMessage &message, bool is_compact_encoding) {

    unsigned char metadata_tagbyte;
    size_t read_length;
    metadata_tagbyte = read_byte(reader);
    // Check the metadata byte
    if(metadata_tagbyte != JSON_ENCODING) {
        return false;
    }

    unsigned char length_data_type = read_byte(reader);

    constexpr int METADATA_LEN_UBYTE = 0x11;
    constexpr int METADATA_LEN_USHORT = 0x12;
    constexpr int METADATA_LEN_INT = 0x13;

    unsigned int metadata_length;
    switch(length_data_type) {
        case METADATA_LEN_UBYTE:
            metadata_length = read_byte(reader);
            break;
        case METADATA_LEN_USHORT:
            metadata_length = read_short(reader);
            break;
        case METADATA_LEN_INT:
            metadata_length = read_unsigned(reader);
            break;
        default:
            printf("error\n");
    }

    std::vector<char> buffer_vec(metadata_length);
    auto error_code = reader.try_read(buffer_vec.data(), metadata_length, read_length);
    if (ErrorCode_Success != error_code) {
        SPDLOG_ERROR("Failed to read metadata");
    }
    std::string buffer_str(buffer_vec.data(), metadata_length);
    auto j3 = nlohmann::json::parse(buffer_str);
    // TODO: LET IT USE THE TRUE TIMESTAMP FORMAT
    // std::string time_stamp_string = j3.at("TIMESTAMP_PATTERN");
    std::string timezone_id = j3.at("TZ_ID");
    std::string encode_version = j3.at("VERSION");
    std::string time_stamp_string;
    if(is_compact_encoding) {
        std::string reference_timestamp = j3.at("REFERENCE_TIMESTAMP");
        epochtime_t reference_ts = boost::lexical_cast<epochtime_t>(reference_timestamp);
        m_last_timestamp = reference_ts;
        time_stamp_string = "%Y-%m-%dT%H:%M:%S.%3Z";
    } else {
        m_last_timestamp = 0;
        time_stamp_string = "%y/%m/%d %H:%M:%S";
    }
    message.set_ts_pattern(0, time_stamp_string);
    m_timezone = timezone_id;
    m_version = encode_version;
    m_compact_encoding = is_compact_encoding;
    message.set_compact(is_compact_encoding);
    return true;
}