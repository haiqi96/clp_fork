#include "STDIRDecompressor.hpp"
#include "../../submodules/json/single_include/nlohmann/json.hpp"
#include <vector>
#include "../type_utils.hpp"
#include "../ffi/ir_stream/encoding_methods.hpp"
#include "../ffi/ir_stream/byteswap.hpp"
// spdlog
#include <spdlog/spdlog.h>

namespace clp {
    void STDIRDecompressor::open (const std::string& path, FileWriter::OpenMode open_mode) {
        m_decompressed_file_writer.open(path, open_mode);
        m_zstd_ir_compressor.open(m_decompressed_file_writer);
    }

    void STDIRDecompressor::write_eof_and_close () {
        m_zstd_ir_compressor.write_char(char(ffi::ir_stream::cProtocol::Eof));
        m_zstd_ir_compressor.close();
        m_decompressed_file_writer.close();
    }

    void STDIRDecompressor::close_without_eof () {
        m_zstd_ir_compressor.close();
        m_decompressed_file_writer.close();
    }

    void STDIRDecompressor::write_msg (const streaming_archive::reader::IRMessage& ir_msg) {

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

        epochtime_t msg_timestamp = ir_msg.get_timestamp();
        write_timestamp(msg_timestamp);
    }

    void STDIRDecompressor::write_timestamp (epochtime_t timestamp_value) {
        // Encode timestamp delta
        m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::TimestampVal);
        encode_int(timestamp_value);
    }

    void STDIRDecompressor::write_logtype (const std::string& logtype) {
        auto length = logtype.length();
        if (length <= UINT8_MAX) {
            m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::LogtypeStrLenUByte);
            encode_int(static_cast<uint8_t>(length));
        } else if (length <= UINT16_MAX) {
            m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::LogtypeStrLenUShort);
            encode_int(static_cast<uint16_t>(length));
        } else if (length <= INT32_MAX) {
            m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::LogtypeStrLenInt);
            encode_int(static_cast<int32_t>(length));
        } else {
            SPDLOG_ERROR("Logtype entry length out of bound");
            throw;
        }
        m_zstd_ir_compressor.write_string(logtype);
    }

    void STDIRDecompressor::write_encoded_var (encoded_variable_t encoded_var) {
        m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::VarEightByteEncoding);
        encode_int(encoded_var);
    }

    void STDIRDecompressor::write_dict_var (const std::string& dict_var) {
        auto length = dict_var.length();
        if (length <= UINT8_MAX) {
            m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::VarStrLenUByte);
            encode_int(static_cast<uint8_t>(length));
        } else if (length <= UINT16_MAX) {
            m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::VarStrLenUShort);
            encode_int(static_cast<uint16_t>(length));
        } else if (length <= INT32_MAX) {
            m_zstd_ir_compressor.write_char((char)ffi::ir_stream::cProtocol::Payload::VarStrLenInt);
            encode_int(static_cast<int32_t>(length));
        } else {
            SPDLOG_ERROR("Dictionary entry length out of bound");
            throw;
        }
        m_zstd_ir_compressor.write_string(dict_var);
    }

    template <typename integer_t>
    void STDIRDecompressor::encode_int (integer_t value) {
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
        m_zstd_ir_compressor.write(data, sizeof(value));
    }

    bool STDIRDecompressor::write_premable (const std::string& timestamp_pattern,
                                            const std::string& timestamp_pattern_syntax,
                                            const std::string& timezone) {
        std::vector<int8_t> ir_buf;
        bool result = ffi::ir_stream::eight_byte_encoding::encode_preamble(timestamp_pattern, timestamp_pattern_syntax, timezone, ir_buf);
        if(result) {
            m_zstd_ir_compressor.write(reinterpret_cast<const char*>(ir_buf.data()),
                                       ir_buf.size());
        }
        return result;
    }
}