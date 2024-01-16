#include "Archive.hpp"

// C libraries
#include <sys/stat.h>

// C++ libraries
#include <cstring>
#include <fstream>
#include <vector>

// spdlog
#include <spdlog/spdlog.h>

// Project headers
#include "../../EncodedVariableInterpreter.hpp"
#include "../../Utils.hpp"
#include "../Constants.hpp"

using std::string;
using std::unordered_set;
using std::vector;

namespace streaming_archive::reader {

    void Archive::read_metadata_file (const string& path, archive_format_version_t& format_version, size_t& stable_uncompressed_size, size_t& stable_size) {
        FileReader file_reader;
        file_reader.open(path);
        file_reader.read_numeric_value(format_version, false);
        file_reader.read_numeric_value(stable_uncompressed_size, false);
        file_reader.read_numeric_value(stable_size, false);
        file_reader.close();
    }

    void Archive::open (const string& path) {
        // Determine whether path is file or directory
        struct stat path_stat = {};
        const char* path_c_str = path.c_str();
        if (0 != stat(path_c_str, &path_stat)) {
            SPDLOG_ERROR("Failed to stat {}, errno={}", path_c_str, errno);
            throw OperationFailed(ErrorCode_errno, __FILENAME__, __LINE__);
        }
        if (!S_ISDIR(path_stat.st_mode)) {
            SPDLOG_ERROR("{} is not a directory", path_c_str);
            throw OperationFailed(ErrorCode_Unsupported, __FILENAME__, __LINE__);
        }
        m_path = path;

        // Read the metadata file
        string metadata_file_path = path + '/' + cMetadataFileName;
        uint16_t format_version;
        size_t stable_uncompressed_size;
        size_t stable_size;
        try {
            read_metadata_file(metadata_file_path, format_version, stable_uncompressed_size, stable_size);
        } catch (TraceableException& traceable_exception) {
            auto error_code = traceable_exception.get_error_code();
            if (ErrorCode_errno == error_code) {
                SPDLOG_CRITICAL("streaming_archive::reader::Archive: Failed to read archive metadata file {} at {}:{} - errno={}", metadata_file_path.c_str(),
                                traceable_exception.get_filename(), traceable_exception.get_line_number(), errno);
            } else {
                SPDLOG_CRITICAL("streaming_archive::reader::Archive: Failed to read archive metadata file {} at {}:{} - error={}", metadata_file_path.c_str(),
                                traceable_exception.get_filename(), traceable_exception.get_line_number(), error_code);
            }
            throw;
        }

        // Check archive matches format version
        if (cArchiveFormatVersion != format_version) {
            SPDLOG_ERROR("streaming_archive::reader::Archive: Archive uses an unsupported format.");
            throw OperationFailed(ErrorCode_BadParam, __FILENAME__, __LINE__);
        }

        // Open log-type dictionary
        string logtype_dict_path = m_path;
        logtype_dict_path += '/';
        logtype_dict_path += cLogTypeDictFilename;
        string logtype_segment_index_path = m_path;
        logtype_segment_index_path += '/';
        logtype_segment_index_path += cLogTypeSegmentIndexFilename;
        m_logtype_dictionary.open(logtype_dict_path, logtype_segment_index_path);

        // Open variables dictionary
        string var_dict_path = m_path;
        var_dict_path += '/';
        var_dict_path += cVarDictFilename;
        string var_segment_index_path = m_path;
        var_segment_index_path += '/';
        var_segment_index_path += cVarSegmentIndexFilename;
        m_var_dictionary.open(var_dict_path, var_segment_index_path);

        // Open segment manager
        m_segments_dir_path = m_path;
        m_segments_dir_path += '/';
        m_segments_dir_path += cSegmentsDirname;
        m_segments_dir_path += '/';

        // Open segment list
        string segment_list_path = m_segments_dir_path;
        segment_list_path += cSegmentListFilename;

        // Open metadataDB
        auto metadata_db_path = boost::filesystem::path(path) / cMetadataDBFileName;
        m_metadata_db.open(metadata_db_path.string());

        // Set invalid segment ID
        m_current_segment_id = INT64_MAX;
    }

    void Archive::close () {
        // close GLT
        m_segment.close();
        m_message_order_table.close();

        m_metadata_db.close();

        m_logtype_dictionary.close();
        m_var_dictionary.close();
        m_segments_dir_path.clear();
        m_path.clear();
    }

    void Archive::refresh_dictionaries () {
        m_logtype_dictionary.read_new_entries();
        m_var_dictionary.read_new_entries();
    }

    const LogTypeDictionaryReader& Archive::get_logtype_dictionary () const {
        return m_logtype_dictionary;
    }

    const VariableDictionaryReader& Archive::get_var_dictionary () const {
        return m_var_dictionary;
    }

    bool Archive::decompress_message (File& file, const Message& compressed_msg, string& decompressed_msg) {
        decompressed_msg.clear();

        // Build original message content
        const logtype_dictionary_id_t logtype_id = compressed_msg.get_logtype_id();
        const auto& logtype_entry = m_logtype_dictionary.get_entry(logtype_id);
        if (!EncodedVariableInterpreter::decode_variables_into_message(logtype_entry, m_var_dictionary, compressed_msg.get_vars(), decompressed_msg)) {
            SPDLOG_ERROR("streaming_archive::reader::Archive: Failed to decompress variables from logtype id {}", compressed_msg.get_logtype_id());
            return false;
        }

        // Determine which timestamp pattern to use
        const auto& timestamp_patterns = file.get_timestamp_patterns();
        if (!timestamp_patterns.empty() && compressed_msg.get_message_number() >= timestamp_patterns[file.get_current_ts_pattern_ix()].first) {
            while (true) {
                if (file.get_current_ts_pattern_ix() >= timestamp_patterns.size() - 1) {
                    // Already at last timestamp pattern
                    break;
                }
                auto next_patt_start_message_num = timestamp_patterns[file.get_current_ts_pattern_ix() + 1].first;
                if (compressed_msg.get_message_number() < next_patt_start_message_num) {
                    // Not yet time for next timestamp pattern
                    break;
                }
                file.increment_current_ts_pattern_ix();
            }
            timestamp_patterns[file.get_current_ts_pattern_ix()].second.insert_formatted_timestamp(compressed_msg.get_ts_in_milli(), decompressed_msg);
        }

        return true;
    }

    void Archive::decompress_empty_directories (const string& output_dir) {
        boost::filesystem::path output_dir_path = boost::filesystem::path(output_dir);

        string path;
        auto ix_ptr = m_metadata_db.get_empty_directory_iterator();
        for (auto& ix = *ix_ptr; ix.has_next(); ix.next()) {
            ix.get_path(path);
            auto empty_directory_path = output_dir_path / path;
            auto error_code = create_directory_structure(empty_directory_path.string(), 0700);
            if (ErrorCode_Success != error_code) {
                SPDLOG_ERROR("Failed to create directory structure {}, errno={}", empty_directory_path.string().c_str(), errno);
                throw OperationFailed(error_code, __FILENAME__, __LINE__);
            }
        }
    }

    // GLT functions
    ErrorCode Archive::open_file (File& file, MetadataDB::FileIterator& file_metadata_ix) {
        const auto segment_id = file_metadata_ix.get_segment_id();
        if (segment_id != m_current_segment_id) {
            if (m_current_segment_id != INT64_MAX) {
                m_segment.close();
                m_message_order_table.close();
            }
            ErrorCode error_code = m_segment.try_open(m_segments_dir_path, segment_id);
            if(error_code != ErrorCode_Success) {
                m_segment.close();
                return error_code;
            }
            error_code = m_message_order_table.try_open(m_segments_dir_path, segment_id);
            if(error_code != ErrorCode_Success) {
                m_message_order_table.close();
                m_segment.close();
                return error_code;
            }
            m_current_segment_id = segment_id;
        }
        return file.open(m_logtype_dictionary, file_metadata_ix, m_segment, m_message_order_table);
    }

    void Archive::close_file (File& file) {
        file.close();
    }

    void Archive::reset_file_indices (File& file) {
        file.reset_indices();
    }


    bool Archive::get_next_message (File& file, Message& msg) {
        return file.get_next_message(msg);
    }
}