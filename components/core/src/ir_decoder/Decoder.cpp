#include "Decoder.hpp"

#include <iostream>
#include <set>

// Boost libraries
#include <boost/filesystem/path.hpp>

// libarchive
#include <archive_entry.h>
#include "../ffi/ir_stream/protocol_constants.hpp"
using ffi::ir_stream::cProtocol::MagicNumberLength;
using ffi::ir_stream::cProtocol::EightByteEncodingMagicNumber;
using ffi::ir_stream::cProtocol::FourByteEncodingMagicNumber;

namespace ir_decoder {

    encoded_variable_t Decoder::convert_eightbytes_to_fourbytes(encoded_variable_t eightbyte_encoded_var) {

    }

    bool Decoder::is_clp_magic_number(size_t sequence_length, const char* sequence, bool& is_compacted) {
        if(0 == memcmp(EightByteEncodingMagicNumber, sequence, MagicNumberLength)) {
            is_compacted = false;
            return true;
        } else if (0 == memcmp(FourByteEncodingMagicNumber, sequence, MagicNumberLength)) {
            is_compacted = true;
            return true;
        }
        return false;
    }

    bool Decoder::decode (std::string input_path, std::string output_path) {
        m_file_reader.open(input_path);
        // For decode, support plain text for now. but we can always remove it later.
        auto error_code = m_file_reader.try_read(m_clp_custom_encoding_buf, cCLPMagicNumberBufCapacity, m_clp_custom_buf_length);
        if (ErrorCode_Success != error_code) {
            if (ErrorCode_EndOfFile != error_code) {
                SPDLOG_ERROR("Failed to read {}, errno={}", input_path.c_str(), errno);
                return false;
            }
        }
        m_file_writer.open(output_path, FileWriter::OpenMode::CREATE_FOR_WRITING);

        bool succeeded = true;
        bool is_compacted_encoding = false;
        if (is_clp_magic_number(m_clp_custom_buf_length, m_clp_custom_encoding_buf, is_compacted_encoding)) {
            parse_and_decode(m_file_reader, is_compacted_encoding);
        } else {
            if (false == try_compressing_as_archive(input_path))
            {
                SPDLOG_ERROR("Failed to handle zstd");
                succeeded = false;
            }
        }

        m_file_writer.close();
        m_file_reader.close();

        return succeeded;
    }

    bool Decoder::try_compressing_as_archive (std::string input_path) {
        auto file_boost_path = boost::filesystem::path(input_path);
        auto parent_boost_path = file_boost_path.parent_path();

        // Determine path without extension (used if file is a single compressed file, e.g., syslog.gz -> syslog)
        std::string filename_if_compressed;
        if (file_boost_path.has_stem()) {
            filename_if_compressed = file_boost_path.stem().string();
        } else {
            filename_if_compressed = file_boost_path.filename().string();
        }

        // TODO: theratically we don't need m_archive_validation_buf but can't get it working atm
        // so for now, read some data into m_archive_validation_buf.
        m_file_reader.seek_from_begin(0);
        auto error_code = m_file_reader.try_read(m_archive_validation_buf, cArchiveValidationBufCapacity, m_validation_buf_length);
        if (ErrorCode_Success != error_code) {
            if (ErrorCode_EndOfFile != error_code) {
                SPDLOG_ERROR("Failed to read {}, errno={}", input_path.c_str(), errno);
                return false;
            }
        }
        error_code = m_libarchive_reader.try_open(m_validation_buf_length, m_archive_validation_buf, m_file_reader, filename_if_compressed);
        if (ErrorCode_Success != error_code) {
            SPDLOG_ERROR("Cannot compress {} - not UTF-8 encoded.", input_path.c_str());
            return false;
        }

        // Compress each file and directory in the archive
        bool succeeded;
        std::set<std::string> directories;
        std::set<std::string> parent_directories;
        while (true) {
            error_code = m_libarchive_reader.try_read_next_header();
            if (ErrorCode_Success != error_code) {
                if (ErrorCode_EndOfFile == error_code) {
                    break;
                }
                SPDLOG_ERROR("Failed to read entry in {}.", input_path.c_str());
                succeeded = false;
                break;
            }

            // Determine what type of file it is
            auto file_type = m_libarchive_reader.get_entry_file_type();
            if (AE_IFREG != file_type) {
                SPDLOG_ERROR("Not supporting archive with multiple files at the moment");
                continue;
            }

            m_libarchive_reader.open_file_reader(m_libarchive_file_reader);

            // Check that file is CLP encoded
            auto error_code = m_libarchive_file_reader.try_read(m_clp_custom_encoding_buf, cCLPMagicNumberBufCapacity, m_clp_custom_buf_length);
            if (ErrorCode_Success != error_code) {
                if (ErrorCode_EndOfFile != error_code) {
                    SPDLOG_ERROR("Failed to read {}, errno={}", input_path, errno);
                    return false;
                }
            }

            succeeded = true;
            bool is_compacted_encoding = false;
            if (is_clp_magic_number(m_clp_custom_buf_length, m_clp_custom_encoding_buf, is_compacted_encoding)) {
                auto boost_path_for_compression = parent_boost_path / m_libarchive_reader.get_path();
                parse_and_decode(m_libarchive_file_reader, is_compacted_encoding);
            } else {
                SPDLOG_ERROR("Cannot compress {} - not clp encoded", m_libarchive_reader.get_path());
                succeeded = false;
                break;
            }

            m_libarchive_file_reader.close();
        }

        m_libarchive_reader.close();

        return succeeded;
    }

    void Decoder::parse_and_decode (ReaderInterface& reader, bool is_compact_encoding)
    {
        m_encoded_parsed_message.clear();
        m_encoded_parsed_message.set_compact(is_compact_encoding);
        // ToDo: Open compressed file

        // Parse content from metadat
        if(!m_encoded_message_parser.parse_metadata(reader, m_encoded_parsed_message, is_compact_encoding)){
            SPDLOG_ERROR("Corrupted metadata.");
        }
        std::string recovered_string;
        // we don't parse the validation buffer anymore because it only contains the magic number
        /* was parsing the validation buffer */
        while (m_encoded_message_parser.parse_next_token(reader, m_encoded_parsed_message)) {
            m_encoded_parsed_message.recover_message(recovered_string);
            m_file_writer.write_string(recovered_string);
        }
    }
}