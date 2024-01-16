//
// Created by haiqi on 2023/1/21.
//

#include "GLTFile.hpp"
#include "../../MetadataDB.hpp"

namespace streaming_archive::reader {
    ErrorCode GLTFile::open (const LogTypeDictionaryReader& archive_logtype_dict,
                             MetadataDB::FileIterator& file_metadata_ix,
                             GLTSegment& segment,
                             Segment& message_order_table) {
        ErrorCode error_code = File::open(archive_logtype_dict, file_metadata_ix);
        if(ErrorCode_Success != error_code) {
            return error_code;
        }
        MetadataDB::FileIterator* glt_file_metadata_ix_ptr = dynamic_cast<MetadataDB::FileIterator*> (&file_metadata_ix);
        m_segment_logtypes_decompressed_stream_pos = glt_file_metadata_ix_ptr->get_segment_logtypes_pos();
        m_segment_offsets_decompressed_stream_pos = glt_file_metadata_ix_ptr->get_segment_offset_pos();

        if (cInvalidSegmentId == m_segment_id) {
            SPDLOG_ERROR("Unexpected invalid segment id");
            return ErrorCode_Truncated;
        }

        uint64_t num_bytes_to_read;
        if (m_num_messages > 0) {
            if (m_num_messages > m_num_segment_msgs) {
                // Buffers too small, so increase size to required amount
                m_segment_logtypes = std::make_unique<logtype_dictionary_id_t[]>(m_num_messages);
                m_segment_offsets = std::make_unique<size_t[]>(m_num_messages);
                m_num_segment_msgs = m_num_messages;
            }
            num_bytes_to_read = m_num_messages*sizeof(logtype_dictionary_id_t);
            error_code = message_order_table.try_read(m_segment_logtypes_decompressed_stream_pos,
                                                      reinterpret_cast<char*>(m_segment_logtypes.get()), num_bytes_to_read);
            if (ErrorCode_Success != error_code) {
                close();
                return error_code;
            }
            m_logtypes = m_segment_logtypes.get();
            num_bytes_to_read = m_num_messages*sizeof(size_t);
            error_code = message_order_table.try_read(m_segment_offsets_decompressed_stream_pos,
                                                      reinterpret_cast<char*>(m_segment_offsets.get()), num_bytes_to_read);
            if (ErrorCode_Success != error_code) {
                close();
                return error_code;
            }
            m_offsets = m_segment_offsets.get();
        }

        m_segment = &segment;

        return ErrorCode_Success;
    }

    void GLTFile::close_derived () {
        m_segment_logtypes_decompressed_stream_pos = 0;
        m_segment_offsets_decompressed_stream_pos = 0;
        m_logtype_table_offsets.clear();
    }

    size_t GLTFile::get_msg_offset (logtype_dictionary_id_t logtype_id, size_t msg_ix) {
        if(m_logtype_table_offsets.find(logtype_id) == m_logtype_table_offsets.end()) {
            m_logtype_table_offsets[logtype_id] = m_offsets[msg_ix];
        }
        size_t return_value = m_logtype_table_offsets[logtype_id];
        m_logtype_table_offsets[logtype_id] += 1;
        return return_value;
    }

    bool GLTFile::get_next_message (Message& msg) {
        if (m_msgs_ix >= m_num_messages) {
            return false;
        }

        // Get message number
        msg.set_message_number(m_msgs_ix);

        // Get log-type
        auto logtype_id = m_logtypes[m_msgs_ix];
        msg.set_logtype_id(logtype_id);

        // Get variables
        msg.clear_vars();
        const auto& logtype_dictionary_entry = m_archive_logtype_dict->get_entry(logtype_id);

        // Get timestamp
        auto variable_offset = get_msg_offset(logtype_id, m_msgs_ix);
        auto timestamp = m_segment->get_timestamp_at_offset(logtype_id, variable_offset);
        msg.set_timestamp(timestamp);

        auto num_vars = logtype_dictionary_entry.get_num_vars();
        if(num_vars > 0) {
            // The behavior here slight changed. the function will throw an error
            // if the attempt to load variable fails
            m_segment->get_variable_row_at_offset(logtype_id, variable_offset, msg);
        }

        ++m_msgs_ix;

        return true;
    }
}