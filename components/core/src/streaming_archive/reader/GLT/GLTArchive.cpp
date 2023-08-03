#include "GLTArchive.hpp"

namespace streaming_archive::reader::glt {
    void GLTArchive::open_derived (const std::string& path) {
        m_metadata_db = std::make_unique<GLTMetadataDB>();
        auto metadata_db_path = boost::filesystem::path(path) / cMetadataDBFileName;
        m_metadata_db->open(metadata_db_path.string());
    }

    void GLTArchive::close_derived () {
        m_metadata_db->close();
        m_segment.close();
        m_message_order_table.close();
        m_current_segment_id = INT64_MAX;
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
}