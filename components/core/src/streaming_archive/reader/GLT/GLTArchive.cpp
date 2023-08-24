#include "GLTArchive.hpp"
// msgpack
#include <msgpack.hpp>
#include "../../../networking/socket_utils.hpp"
#include "../../../EncodedVariableInterpreter.hpp"

namespace streaming_archive::reader::glt {
    void GLTArchive::open_derived (const std::string& path) {
        m_metadata_db = std::make_unique<GLTMetadataDB>();
        auto metadata_db_path = boost::filesystem::path(path) / cMetadataDBFileName;
        m_metadata_db->open(metadata_db_path.string());

        update_valid_segment_ids();
        load_filename_dict();
    }

    void GLTArchive::close_derived () {
        m_metadata_db->close();
        m_filename_dict.clear();
    }

    ErrorCode GLTArchive::open_file (GLTFile& file, MetadataDB::FileIterator& file_metadata_ix) {
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

    void GLTArchive::close_file (GLTFile& file) {
        file.close();
    }

    void GLTArchive::reset_file_indices (GLTFile& file) {
        file.reset_indices();
    }

    bool GLTArchive::get_next_message (GLTFile& file, GLTMessage& msg) {
        return file.get_next_message(msg);
    }

    void GLTArchive::open_table_manager(size_t segment_id) {
        std::string segment_path = m_segments_dir_path + std::to_string(segment_id);
        m_single_table_manager.open(segment_path);
    }

    void GLTArchive::close_table_manager() {
        m_single_table_manager.close();
    }

    std::string GLTArchive::get_file_name (file_id_t file_id) const {
        if(file_id >= m_filename_dict.size()) {
            SPDLOG_ERROR("file id {} out of bound", file_id);
            throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
        }
        return m_filename_dict[file_id];
    }

    void GLTArchive::load_filename_dict () {
        FileReader filename_dict_reader;
        std::string filename_dict_path = m_path + '/' + cFileNameDictFilename;
        filename_dict_reader.open(filename_dict_path);
        std::string file_name;

        while(true) {
            auto errorcode = filename_dict_reader.try_read_to_delimiter('\n',false, false, file_name);
            if (errorcode == ErrorCode_Success) {
                m_filename_dict.push_back(file_name);
            } else if (errorcode == ErrorCode_EndOfFile) {
                break;
            } else {
                SPDLOG_ERROR("Failed to read from {}, errno={}", filename_dict_path.c_str(), errno);
                throw OperationFailed(errorcode, __FILENAME__, __LINE__);
            }
        }
        filename_dict_reader.close();
    }

    void GLTArchive::update_valid_segment_ids () {
        m_valid_segment_id.clear();
        // Better question here is why we produce 0 size segment
        size_t segment_count = 0;
        while(true) {
            std::string segment_file_path = m_segments_dir_path + "/" + std::to_string(segment_count);
            if (!boost::filesystem::exists(segment_file_path))
            {
                break;
            }
            boost::system::error_code boost_error_code;
            size_t segment_file_size = boost::filesystem::file_size(segment_file_path, boost_error_code);
            if (boost_error_code) {
                SPDLOG_ERROR("streaming_archive::reader::Segment: Unable to obtain file size for segment: {}", segment_file_path.c_str());
                SPDLOG_ERROR("streaming_archive::reader::Segment: {}", boost_error_code.message().c_str());
                throw ErrorCode_Failure;
            }
            if (segment_file_size != 0) {
                m_valid_segment_id.push_back(segment_count);
            }
            segment_count++;
        }
    }

    bool GLTArchive::get_next_message_with_logtype(GLTMessage& msg) {
        return m_single_table_manager.get_next_row(msg);
    }

    void GLTArchive::find_message_matching_with_logtype_query_optimized (const std::vector<LogtypeQuery>& logtype_query, std::vector<bool>& wildcard, const Query& query, std::vector<size_t>& matched_row) {
        epochtime_t ts;
        size_t num_row = m_single_table_manager.m_single_table.get_num_row();
        size_t num_column = m_single_table_manager.m_single_table.get_num_column();
        std::vector<encoded_variable_t> vars_to_load(num_column);
        for(size_t row_ix = 0; row_ix < num_row; row_ix++) {
            m_single_table_manager.peek_next_ts(ts);
            if (query.timestamp_is_in_search_time_range(ts)) {
                // that means we need to loop through every loop. that takes time.
                for (const auto &possible_sub_query: logtype_query) {
                    m_single_table_manager.m_single_table.get_next_row(vars_to_load, possible_sub_query.m_l_b, possible_sub_query.m_r_b);
                    if (possible_sub_query.matches_vars(vars_to_load)) {
                        // Message matches completely, so set remaining properties
                        wildcard.push_back(possible_sub_query.get_wildcard_flag());
                        matched_row.push_back(row_ix);
                        // don't need to look into other sub-queries as long as there is a match
                        break;
                    }
                }
            }
            m_single_table_manager.skip_row();
        }
    }

    bool GLTArchive::find_message_matching_with_logtype_query_from_combined (const std::vector<LogtypeQuery>& logtype_query, GLTMessage& msg, bool& wildcard, const Query& query, size_t left_boundary, size_t right_boundary) {
        while(true) {
            // break if there's no next message
            if(!m_single_table_manager.m_combined_table.get_next_message_partial(msg, left_boundary, right_boundary)) {
                break;
            }

            if (query.timestamp_is_in_search_time_range(msg.get_ts_in_milli())) {
                for (const auto &possible_sub_query: logtype_query) {
                    if (possible_sub_query.matches_vars(msg.get_vars())) {
                        // Message matches completely, so set remaining properties
                        wildcard = possible_sub_query.get_wildcard_flag();
                        m_single_table_manager.m_combined_table.get_remaining_message(msg, left_boundary, right_boundary);
                        return true;
                    }
                }
            }
            // if there is no match, skip next row
            m_single_table_manager.m_combined_table.skip_next_row();
        }
        return false;
    }

    bool GLTArchive::find_message_matching_with_logtype_query (const std::vector<LogtypeQuery>& logtype_query, GLTMessage& msg, bool& wildcard, const Query& query) {
        while(true) {
            if(!m_single_table_manager.get_next_row(msg)) {
                break;
            }

            if (query.timestamp_is_in_search_time_range(msg.get_ts_in_milli())) {
                // that means we need to loop through every loop. that takes time.
                for (const auto &possible_sub_query: logtype_query) {
                    if (possible_sub_query.matches_vars(msg.get_vars())) {
                        // Message matches completely, so set remaining properties
                        wildcard = possible_sub_query.get_wildcard_flag();
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // almost a copy of decompress_messages_and_output except we change the output method to write to network
    ErrorCode GLTArchive::decompress_messages_and_send_result (logtype_dictionary_id_t logtype_id, std::vector<epochtime_t>& ts, std::vector<file_id_t>& id,
                                                           std::vector<encoded_variable_t>& vars, std::vector<bool>& wildcard_required,
                                                         const Query& query, const std::atomic_bool& query_cancelled, int controller_socket_fd) {
        const auto& logtype_entry = m_logtype_dictionary.get_entry(logtype_id);
        size_t num_vars = logtype_entry.get_num_vars();
        const size_t total_matches = wildcard_required.size();
        std::string decompressed_msg;
        for(size_t ix = 0; ix < total_matches; ix++) {
            if(query_cancelled) {
                break;
            }
            decompressed_msg.clear();

            // first decompress the message with fixed time stamp
            size_t vars_offset = num_vars * ix;
            if (!EncodedVariableInterpreter::decode_variables_into_message(logtype_entry, m_var_dictionary, vars, decompressed_msg, vars_offset)) {
                SPDLOG_ERROR("streaming_archive::reader::Archive: Failed to decompress variables from logtype id {}", logtype_id);
                return ErrorCode_Failure;
            }

            // Perform wildcard match if required
            // Check if:
            // - Sub-query requires wildcard match, or
            // - no subqueries exist and the search string is not a match-all
            if ((query.contains_sub_queries() && wildcard_required[ix]) ||
                (query.contains_sub_queries() == false && query.search_string_matches_all() == false)) {
                bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                                     query.get_ignore_case() == false);
                if (!matched) {
                    continue;
                }
            }
            std::string orig_file_path = get_file_name(id[ix]);
            // send match
            msgpack::type::tuple<std::string, epochtime_t, std::string> src(orig_file_path, ts[ix], decompressed_msg);
            msgpack::sbuffer m;
            msgpack::pack(m, src);
            ErrorCode ret = networking::try_send(controller_socket_fd, m.data(), m.size());
            if(ret != ErrorCode_Success) {
                return ret;
            }
        }
        return ErrorCode_Success;
    }

    size_t GLTArchive::decompress_messages_and_output (logtype_dictionary_id_t logtype_id, std::vector<epochtime_t>& ts, std::vector<file_id_t>& id,
                                                   std::vector<encoded_variable_t>& vars, std::vector<bool>& wildcard_required, const Query& query) {
        const auto& logtype_entry = m_logtype_dictionary.get_entry(logtype_id);
        size_t num_vars = logtype_entry.get_num_vars();
        const size_t total_matches = wildcard_required.size();
        std::string decompressed_msg;
        size_t matches = 0;
        for(size_t ix = 0; ix < total_matches; ix++) {
            decompressed_msg.clear();

            // first decompress the message with fixed time stamp
            size_t vars_offset = num_vars * ix;
            if (!EncodedVariableInterpreter::decode_variables_into_message(logtype_entry, m_var_dictionary, vars, decompressed_msg, vars_offset)) {
                SPDLOG_ERROR("streaming_archive::reader::Archive: Failed to decompress variables from logtype id {}", logtype_id);
                throw OperationFailed(ErrorCode_Failure, __FILENAME__, __LINE__);
            }
            const std::string fixed_timestamp_pattern = "%Y-%m-%d %H:%M:%S,%3";
            TimestampPattern ts_pattern(0, fixed_timestamp_pattern);
            ts_pattern.insert_formatted_timestamp(ts[ix], decompressed_msg);

            // Perform wildcard match if required
            // Check if:
            // - Sub-query requires wildcard match, or
            // - no subqueries exist and the search string is not a match-all
            if ((query.contains_sub_queries() && wildcard_required[ix]) ||
                (query.contains_sub_queries() == false && query.search_string_matches_all() == false)) {
                bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                                     query.get_ignore_case() == false);
                if (!matched) {
                    continue;
                }
            }
            matches++;
            std::string orig_file_path = get_file_name(id[ix]);
            // Print match
            printf("%s:%s", orig_file_path.c_str(), decompressed_msg.c_str());
        }
        return matches;
    }

    bool GLTArchive::decompress_message_with_fixed_timestamp_pattern (const Message& compressed_msg, std::string& decompressed_msg) {
        decompressed_msg.clear();

        // Build original message content
        const logtype_dictionary_id_t logtype_id = compressed_msg.get_logtype_id();
        const auto& logtype_entry = m_logtype_dictionary.get_entry(logtype_id);
        if (!EncodedVariableInterpreter::decode_variables_into_message(logtype_entry, m_var_dictionary, compressed_msg.get_vars(), decompressed_msg)) {
            SPDLOG_ERROR("streaming_archive::reader::Archive: Failed to decompress variables from logtype id {}", compressed_msg.get_logtype_id());
            return false;
        }
        const std::string fixed_timestamp_pattern = "%Y-%m-%d %H:%M:%S,%3";
        TimestampPattern ts_pattern(0, fixed_timestamp_pattern);
        ts_pattern.insert_formatted_timestamp(compressed_msg.get_ts_in_milli(), decompressed_msg);
        return true;
    }
}