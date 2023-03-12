#include "IRDecompressor.hpp"
#include "../../submodules/json/single_include/nlohmann/json.hpp"
#include <vector>
#include "../type_utils.hpp"
#include "../ffi/ir_stream/encoding_methods.hpp"
#include "../ffi/ir_stream/byteswap.hpp"
// spdlog
#include <spdlog/spdlog.h>

namespace clp {
    void IRDecompressor::open (const std::string& path, FileWriter::OpenMode open_mode) {
        m_decompressed_file_writer.open(path, open_mode);
    }

    void IRDecompressor::close () {
        m_decompressed_file_writer.write_char(char(ffi::ir_stream::cProtocol::Eof));
        m_decompressed_file_writer.close();
        last_ts = 0;
    }

    void IRDecompressor::write_msg (const streaming_archive::reader::IRMessage& ir_msg) {

        const auto& dict_vars = ir_msg.get_dictionary_vars();
        size_t dict_var_ix = 0;
        const auto& encoded_vars = ir_msg.get_encoded_vars();
        size_t encoded_var_ix = 0;
        const auto& var_types = ir_msg.get_var_types();
        // write all variables
        for(auto is_dict : var_types) {
            if(is_dict) {
                write_dict_var(dict_vars.at(dict_var_ix++));
            } else {
                write_encoded_var(encoded_vars.at(encoded_var_ix++));
            }
        }
        write_logtype(ir_msg.get_log_type());

        epochtime_t timestamp_delta = ir_msg.get_timestamp() - last_ts;
        last_ts = ir_msg.get_timestamp();
        write_timestamp(timestamp_delta);


    }

    void IRDecompressor::write_timestamp (epochtime_t timestamp_delta) {
        // Encode timestamp delta
        if (INT8_MIN <= timestamp_delta && timestamp_delta <= INT8_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::TimestampDeltaByte);
            encode_int(static_cast<int8_t>(timestamp_delta));
        } else if (INT16_MIN <= timestamp_delta && timestamp_delta <= INT16_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::TimestampDeltaShort);
            encode_int(static_cast<int16_t>(timestamp_delta));
        } else if (INT32_MIN <= timestamp_delta && timestamp_delta <= INT32_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::TimestampDeltaInt);
            encode_int(static_cast<int32_t>(timestamp_delta));
        } else {
            SPDLOG_ERROR("timestamp delta out of bound");
            throw;
        }
    }

    void IRDecompressor::write_logtype (const std::string& logtype) {
        auto length = logtype.length();
        if (length <= UINT8_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::LogtypeStrLenUByte);
            encode_int(static_cast<uint8_t>(length));
        } else if (length <= UINT16_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::LogtypeStrLenUShort);
            encode_int(static_cast<uint16_t>(length));
        } else if (length <= INT32_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::LogtypeStrLenInt);
            encode_int(static_cast<int32_t>(length));
        } else {
            SPDLOG_ERROR("Logtype entry length out of bound");
            throw;
        }
        m_decompressed_file_writer.write_string(logtype);
    }

    void IRDecompressor::write_encoded_var (encoded_variable_t encoded_var) {
        m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::VarFourByteEncoding);
        encode_int(static_cast<int32_t>(encoded_var));
    }

    void IRDecompressor::write_dict_var (const std::string& dict_var) {
        auto length = dict_var.length();
        if (length <= UINT8_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::VarStrLenUByte);
            encode_int(static_cast<uint8_t>(length));
        } else if (length <= UINT16_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::VarStrLenUShort);
            encode_int(static_cast<uint16_t>(length));
        } else if (length <= INT32_MAX) {
            m_decompressed_file_writer.write_char((char)ffi::ir_stream::cProtocol::Payload::VarStrLenInt);
            encode_int(static_cast<int32_t>(length));
        } else {
            SPDLOG_ERROR("Dictionary entry length out of bound");
            throw;
        }
        m_decompressed_file_writer.write_string(dict_var);
    }

    template <typename integer_t>
    void IRDecompressor::encode_int (integer_t value) {
        integer_t value_big_endian;
        static_assert(sizeof(integer_t) == 1 || sizeof(integer_t) == 2 || sizeof(integer_t) == 4 || sizeof(integer_t) == 8);
        if constexpr (sizeof(value) == 1) {
            value_big_endian = value;
        } else if constexpr (sizeof(value) == 2) {
            value_big_endian = bswap_16(value);
        } else if constexpr (sizeof(value) == 4) {
            value_big_endian = bswap_32(value);
        } else if constexpr (sizeof(value) == 8) {
            value_big_endian = bswap_64(value);
        }
        auto data = reinterpret_cast<char*>(&value_big_endian);
        m_decompressed_file_writer.write(data, sizeof(value));
    }

    bool IRDecompressor::write_premable (epochtime_t reference_ts,
                                         const std::string& timestamp_pattern,
                                         const std::string& timestamp_pattern_syntax,
                                         const std::string& timezone) {
        std::vector<int8_t> ir_buf;
        bool result = ffi::ir_stream::four_byte_encoding::encode_preamble(timestamp_pattern, timestamp_pattern_syntax, timezone, reference_ts, ir_buf);
        if(result) {
            m_decompressed_file_writer.write(reinterpret_cast<const char*>(ir_buf.data()),
                                             ir_buf.size());
        }
        last_ts = reference_ts;
        return result;

//        write_magic_number();
//
//        nlohmann::json metadata;
//        std::vector<int8_t> ir_buf;
//
//        metadata[ffi::ir_stream::cProtocol::Metadata::VersionKey] = ffi::ir_stream::cProtocol::Metadata::VersionValue;
//        metadata[ffi::ir_stream::cProtocol::Metadata::ReferenceTimestampKey] = reference_ts;
//        metadata[ffi::ir_stream::cProtocol::Metadata::TimeZoneIdKey] = timezone;
//        // for now, use some random values
//        metadata[ffi::ir_stream::cProtocol::Metadata::TimestampPatternKey] = "";
//        metadata[ffi::ir_stream::cProtocol::Metadata::TimestampPatternSyntaxKey] = "";
//
//        auto metadata_serialized = metadata.dump(-1, ' ', false, nlohmann::json::error_handler_t::ignore);
//        size_t metadata_serialized_length = metadata_serialized.length();
//
//        ir_buf.push_back(ffi::ir_stream::cProtocol::Metadata::EncodingJson);
//
//        if (metadata_serialized_length <= UINT8_MAX) {
//            ir_buf.push_back(ffi::ir_stream::cProtocol::Metadata::LengthUByte);
//            ir_buf.push_back(bit_cast<int8_t>(static_cast<uint8_t>(metadata_serialized_length)));
//        } else if (metadata_serialized_length <= UINT16_MAX) {
//            ir_buf.push_back(ffi::ir_stream::cProtocol::Metadata::LengthUShort);
//            encode_int(static_cast<uint16_t>(metadata_serialized_length), ir_buf);
//        } else {
//            // Can't encode metadata longer than 64 KiB
//            return false;
//        }
//        ir_buf.insert(ir_buf.cend(), metadata_serialized.cbegin(), metadata_serialized.cend());

    }
}