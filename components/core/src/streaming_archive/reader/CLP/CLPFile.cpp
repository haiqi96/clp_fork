#include "CLPFile.hpp"
#include "../../CLPMetadataDB.hpp"

using streaming_archive::clp::CLPMetadataDB;

namespace streaming_archive::reader::clp {
    ErrorCode CLPFile::open (const LogTypeDictionaryReader& archive_logtype_dict,
                             MetadataDB::FileIterator& file_metadata_ix,
                             SegmentManager& segment_manager) {
        ErrorCode error_code = File::open(archive_logtype_dict, file_metadata_ix);
        if(ErrorCode_Success != error_code) {
            return error_code;
        }
        m_num_variables = file_metadata_ix.get_num_variables();
        CLPMetadataDB::CLPFileIterator* clp_file_metadata_ix_ptr = dynamic_cast<CLPMetadataDB::CLPFileIterator*> (&file_metadata_ix);
        m_segment_timestamps_decompressed_stream_pos = clp_file_metadata_ix_ptr->get_segment_timestamps_pos();
        m_segment_logtypes_decompressed_stream_pos = clp_file_metadata_ix_ptr->get_segment_logtypes_pos();
        m_segment_variables_decompressed_stream_pos = clp_file_metadata_ix_ptr->get_segment_variables_pos();

        uint64_t num_bytes_to_read;
        if (m_num_messages > 0) {
            if (m_num_messages > m_num_segment_msgs) {
                // Buffers too small, so increase size to required amount
                m_segment_timestamps = std::make_unique<epochtime_t[]>(m_num_messages);
                m_segment_logtypes = std::make_unique<logtype_dictionary_id_t[]>(m_num_messages);
                m_num_segment_msgs = m_num_messages;
            }

            num_bytes_to_read = m_num_messages*sizeof(epochtime_t);
            error_code = segment_manager.try_read(
                    m_segment_id,
                    m_segment_timestamps_decompressed_stream_pos,
                    reinterpret_cast<char*>(m_segment_timestamps.get()),
                    num_bytes_to_read
            );
            if (ErrorCode_Success != error_code) {
                close();
                return error_code;
            }
            m_timestamps = m_segment_timestamps.get();

            num_bytes_to_read = m_num_messages*sizeof(logtype_dictionary_id_t);
            error_code = segment_manager.try_read(
                    m_segment_id,
                    m_segment_logtypes_decompressed_stream_pos,
                    reinterpret_cast<char*>(m_segment_logtypes.get()),
                    num_bytes_to_read
            );
            if (ErrorCode_Success != error_code) {
                close();
                return error_code;
            }
            m_logtypes = m_segment_logtypes.get();
        }

        if (m_num_variables > 0) {
            if (m_num_variables > m_num_segment_vars) {
                // Buffer too small, so increase size to required amount
                m_segment_variables = std::make_unique<encoded_variable_t[]>(m_num_variables);
                m_num_segment_vars = m_num_variables;
            }
            num_bytes_to_read = m_num_variables*sizeof(encoded_variable_t);
            error_code = segment_manager.try_read(
                    m_segment_id,
                    m_segment_variables_decompressed_stream_pos,
                    reinterpret_cast<char*>(m_segment_variables.get()),
                    num_bytes_to_read
            );
            if (ErrorCode_Success != error_code) {
                close();
                return error_code;
            }
            m_variables = m_segment_variables.get();
        }

        m_variables_ix = 0;

        return ErrorCode_Success;
    }

    void CLPFile::close_derived () {
        m_timestamps = nullptr;
        m_logtypes = nullptr;
        m_variables = nullptr;

        m_segment_timestamps_decompressed_stream_pos = 0;
        m_segment_logtypes_decompressed_stream_pos = 0;
        m_segment_variables_decompressed_stream_pos = 0;

        m_variables_ix = 0;
        m_num_variables = 0;
    }


    bool CLPFile::find_message_in_time_range (epochtime_t search_begin_timestamp,
                                              epochtime_t search_end_timestamp, Message& msg)
    {
        bool found_msg = false;
        while (m_msgs_ix < m_num_messages && !found_msg) {
            // Get logtype
            // NOTE: We get the logtype before the timestamp since we need to
            // use it to get the number of variables, and then advance the
            // variable index, regardless of whether the timestamp falls in the
            // time range or not
            auto logtype_id = m_logtypes[m_msgs_ix];

            // Get number of variables in logtype
            const auto& logtype_dictionary_entry = m_archive_logtype_dict->get_entry(logtype_id);
            auto num_vars = logtype_dictionary_entry.get_num_vars();

            auto timestamp = m_timestamps[m_msgs_ix];
            if (search_begin_timestamp <= timestamp && timestamp <= search_end_timestamp) {
                // Get variables
                if (m_variables_ix + num_vars > m_num_variables) {
                    // Logtypes not in sync with variables, so stop search
                    return false;
                }

                msg.clear_vars();
                auto vars_ix = m_variables_ix;
                for (size_t i = 0; i < num_vars; ++i) {
                    auto var = m_variables[vars_ix];
                    ++vars_ix;
                    msg.add_var(var);
                }

                // Set remaining message properties
                msg.set_logtype_id(logtype_id);
                msg.set_timestamp(timestamp);
                msg.set_message_number(m_msgs_ix);

                found_msg = true;
            }

            // Advance indices
            ++m_msgs_ix;
            m_variables_ix += num_vars;
        }

        return found_msg;
    }

    const SubQuery* CLPFile::find_message_matching_query (const Query& query, Message& msg) {
        const SubQuery* matching_sub_query = nullptr;
        while (m_msgs_ix < m_num_messages && nullptr == matching_sub_query) {
            auto logtype_id = m_logtypes[m_msgs_ix];

            // Get number of variables in logtype
            const auto& logtype_dictionary_entry = m_archive_logtype_dict->get_entry(logtype_id);
            auto num_vars = logtype_dictionary_entry.get_num_vars();

            for (auto sub_query : query.get_relevant_sub_queries()) {
                // Check if logtype matches search
                if (sub_query->matches_logtype(logtype_id)) {
                    // Check if timestamp matches
                    auto timestamp = m_timestamps[m_msgs_ix];
                    if (query.timestamp_is_in_search_time_range(timestamp)) {
                        // Get variables
                        if (m_variables_ix + num_vars > m_num_variables) {
                            // Logtypes not in sync with variables, so stop search
                            return nullptr;
                        }

                        msg.clear_vars();
                        auto vars_ix = m_variables_ix;
                        for (size_t i = 0; i < num_vars; ++i) {
                            auto var = m_variables[vars_ix];
                            ++vars_ix;
                            msg.add_var(var);
                        }

                        // Check if variables match
                        if (sub_query->matches_vars(msg.get_vars())) {
                            // Message matches completely, so set remaining properties
                            msg.set_logtype_id(logtype_id);
                            msg.set_timestamp(timestamp);
                            msg.set_message_number(m_msgs_ix);

                            matching_sub_query = sub_query;
                            break;
                        }
                    }
                }
            }

            // Advance indices
            ++m_msgs_ix;
            m_variables_ix += num_vars;
        }

        return matching_sub_query;
    }

    void CLPFile::reset_indices () {
        m_variables_ix = 0;
        m_msgs_ix = 0;
    }

    bool CLPFile::get_next_message (Message& msg) {
        if (m_msgs_ix >= m_num_messages) {
            return false;
        }

        // Get message number
        msg.set_message_number(m_msgs_ix);

        // Get timestamp
        msg.set_timestamp(m_timestamps[m_msgs_ix]);

        // Get log-type
        auto logtype_id = m_logtypes[m_msgs_ix];
        msg.set_logtype_id(logtype_id);

        // Get variables
        msg.clear_vars();
        const auto& logtype_dictionary_entry = m_archive_logtype_dict->get_entry(logtype_id);
        auto num_vars = logtype_dictionary_entry.get_num_vars();
        if (m_variables_ix + num_vars > m_num_variables) {
            return false;
        }
        for (size_t i = 0; i < num_vars; ++i) {
            auto var = m_variables[m_variables_ix];
            ++m_variables_ix;
            msg.add_var(var);
        }

        ++m_msgs_ix;

        return true;
    }
}