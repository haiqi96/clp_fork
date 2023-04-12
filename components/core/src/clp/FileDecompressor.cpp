#include "FileDecompressor.hpp"

// Boost libraries
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

// spdlog
#include <spdlog/spdlog.h>
#include <iostream>

using std::string;

namespace clp {
    bool FileDecompressor::decompress_file (streaming_archive::MetadataDB::FileIterator& file_metadata_ix, const string& output_dir,
                                            streaming_archive::reader::clp::CLPArchive& archive_reader, std::unordered_map<string, string>& temp_path_to_final_path)
    {
        // Open compressed file
        auto error_code = archive_reader.open_file(m_encoded_file, file_metadata_ix);
        if (ErrorCode_Success != error_code) {
            if (ErrorCode_errno == error_code) {
                SPDLOG_ERROR("Failed to open encoded file, errno={}", errno);
            } else {
                SPDLOG_ERROR("Failed to open encoded file, error_code={}", error_code);
            }
            return false;
        }

        boost::filesystem::path final_output_path = output_dir;
        final_output_path /= m_encoded_file.get_orig_path();

        boost::filesystem::path temp_output_path = output_dir;
        FileWriter::OpenMode open_mode;
        boost::system::error_code boost_error_code;
        if (m_encoded_file.is_split() || boost::filesystem::exists(final_output_path, boost_error_code)) {
            temp_output_path /= m_encoded_file.get_orig_file_id_as_string();
            open_mode = FileWriter::OpenMode::CREATE_IF_NONEXISTENT_FOR_APPENDING;
            auto temp_output_path_string = temp_output_path.string();
            if (0 == temp_path_to_final_path.count(temp_output_path_string)) {
                temp_path_to_final_path[temp_output_path_string] = final_output_path.string();
            }
        } else {
            temp_output_path = final_output_path;
            open_mode = FileWriter::OpenMode::CREATE_FOR_WRITING;
        }

        // Generate output directory
        error_code = create_directory_structure(final_output_path.parent_path().string(), 0700);
        if (ErrorCode_Success != error_code) {
            SPDLOG_ERROR("Failed to create directory structure {}, errno={}", final_output_path.parent_path().c_str(), errno);
            return false;
        }

        // Open output file
        m_decompressed_file_writer.open(temp_output_path.string(), open_mode);

        // Decompress
        archive_reader.reset_file_indices(m_encoded_file);
        while (archive_reader.get_next_message(m_encoded_file, m_encoded_message)) {
            if (!archive_reader.decompress_message(m_encoded_file, m_encoded_message, m_decompressed_message)) {
                // Can't decompress any more of file
                break;
            }
            m_decompressed_file_writer.write_string(m_decompressed_message);
        }

        // Close files
        m_decompressed_file_writer.close();
        archive_reader.close_file(m_encoded_file);

        return true;
    }


    bool FileDecompressor::decompress_to_4bytes_ir (streaming_archive::MetadataDB::FileIterator& file_metadata_ix, const std::string& output_dir,
                                                    streaming_archive::reader::clp::CLPArchive& archive_reader, std::unordered_map<std::string, std::string>& temp_path_to_final_path,
                                                    std::unordered_map<std::string, epochtime_t>& file_to_last_ts)
    {
        // Open compressed file
        auto error_code = archive_reader.open_file(m_encoded_file, file_metadata_ix);
        if (ErrorCode_Success != error_code) {
            if (ErrorCode_errno == error_code) {
                SPDLOG_ERROR("Failed to open encoded file, errno={}", errno);
            } else {
                SPDLOG_ERROR("Failed to open encoded file, error_code={}", error_code);
            }
            return false;
        }

        std::string file_path;
        file_metadata_ix.get_path(file_path);
        boost::filesystem::path final_output_path = output_dir;
        // TODO: this doesn't really fix the naming problem
        // because the duplicate ID will be added after .clp.zst
        final_output_path /= m_encoded_file.get_orig_path() + ".clp.zst";

        boost::filesystem::path temp_output_path = output_dir;
        FileWriter::OpenMode open_mode;
        boost::system::error_code boost_error_code;
        if (m_encoded_file.is_split() || boost::filesystem::exists(final_output_path, boost_error_code)) {
            temp_output_path /= m_encoded_file.get_orig_file_id_as_string();
            open_mode = FileWriter::OpenMode::CREATE_IF_NONEXISTENT_FOR_APPENDING;
            auto temp_output_path_string = temp_output_path.string();
            if (0 == temp_path_to_final_path.count(temp_output_path_string)) {
                temp_path_to_final_path[temp_output_path_string] = final_output_path.string();
            }
        } else {
            temp_output_path = final_output_path;
            open_mode = FileWriter::OpenMode::CREATE_FOR_WRITING;
        }

        // Generate output directory
        error_code = create_directory_structure(final_output_path.parent_path().string(), 0700);
        if (ErrorCode_Success != error_code) {
            SPDLOG_ERROR("Failed to create directory structure {}, errno={}", final_output_path.parent_path().c_str(), errno);
            return false;
        }

        // Open output file
        m_ir_decompressor.open(temp_output_path.string(), open_mode);

        epochtime_t reference_ts;
        if (m_encoded_file.is_split() && 0 != m_encoded_file.get_split_ix()) {
            reference_ts = file_to_last_ts.at(temp_output_path.string());
            m_ir_decompressor.set_last_ts(reference_ts);
        } else {
            reference_ts = m_encoded_file.get_begin_ts();
            m_ir_decompressor.write_premable(reference_ts, "", "", "");
        }
        // Decompress
        archive_reader.reset_file_indices(m_encoded_file);
        while (archive_reader.get_next_message(m_encoded_file, m_encoded_message)) {
            if (!archive_reader.ir_encode_message(m_encoded_message, m_ir_message)) {
                // Can't decompress any more of file
                break;
            }
            m_ir_decompressor.write_msg(m_ir_message);
        }

        // record timestamp
        file_to_last_ts[temp_output_path.string()] = m_ir_decompressor.get_last_ts();

        // Close files
        m_ir_decompressor.close_without_eof();
        archive_reader.close_file(m_encoded_file);

        return true;
    }

    bool FileDecompressor::decompress_to_ir (streaming_archive::MetadataDB::FileIterator& file_metadata_ix, const std::string& output_dir,
                                             streaming_archive::reader::clp::CLPArchive& archive_reader, std::unordered_map<std::string, std::string>& temp_path_to_final_path,
                                             std::unordered_map<std::string, epochtime_t>& file_to_last_ts)
    {
        // Open compressed file
        auto error_code = archive_reader.open_file(m_encoded_file, file_metadata_ix);
        if (ErrorCode_Success != error_code) {
            if (ErrorCode_errno == error_code) {
                SPDLOG_ERROR("Failed to open encoded file, errno={}", errno);
            } else {
                SPDLOG_ERROR("Failed to open encoded file, error_code={}", error_code);
            }
            return false;
        }

        std::string file_path;
        file_metadata_ix.get_path(file_path);
        boost::filesystem::path final_output_path = output_dir;
        // TODO: this doesn't really fix the naming problem
        // because the duplicate ID will be added after .clp.zst
        final_output_path /= m_encoded_file.get_orig_path() + ".clp.zst";

        boost::filesystem::path temp_output_path = output_dir;
        FileWriter::OpenMode open_mode;
        boost::system::error_code boost_error_code;
        if (m_encoded_file.is_split() || boost::filesystem::exists(final_output_path, boost_error_code)) {
            temp_output_path /= m_encoded_file.get_orig_file_id_as_string();
            open_mode = FileWriter::OpenMode::CREATE_IF_NONEXISTENT_FOR_APPENDING;
            auto temp_output_path_string = temp_output_path.string();
            if (0 == temp_path_to_final_path.count(temp_output_path_string)) {
                temp_path_to_final_path[temp_output_path_string] = final_output_path.string();
            }
        } else {
            temp_output_path = final_output_path;
            open_mode = FileWriter::OpenMode::CREATE_FOR_WRITING;
        }

        // Generate output directory
        error_code = create_directory_structure(final_output_path.parent_path().string(), 0700);
        if (ErrorCode_Success != error_code) {
            SPDLOG_ERROR("Failed to create directory structure {}, errno={}", final_output_path.parent_path().c_str(), errno);
            return false;
        }

        // Open output file
        m_std_ir_decompressor.open(temp_output_path.string(), open_mode);

        epochtime_t reference_ts;
        if (m_encoded_file.is_split() && 0 != m_encoded_file.get_split_ix()) {
            ;
        } else {
            m_std_ir_decompressor.write_premable("", "", "");
        }
        // Decompress
        archive_reader.reset_file_indices(m_encoded_file);
        while (archive_reader.get_next_message(m_encoded_file, m_encoded_message)) {
            if (!archive_reader.ir_encode_std_message(m_encoded_message, m_ir_message)) {
                // Can't decompress any more of file
                break;
            }
            m_std_ir_decompressor.write_msg(m_ir_message);
        }

        // record timestamp
        file_to_last_ts[temp_output_path.string()] = 0;

        // Close files
        m_std_ir_decompressor.close_without_eof();
        archive_reader.close_file(m_encoded_file);

        return true;
    }

}
