//
// Created by haiqi on 2023/1/21.
//

#include "GLTSegment.hpp"
#include "GLTMessage.hpp"

namespace streaming_archive::reader::glt {
    ErrorCode GLTSegment::try_open (const std::string& segment_dir_path, segment_id_t segment_id) {

        std::string segment_path = segment_dir_path + std::to_string(segment_id);
        m_logtype_tables_manager.open(segment_path);

        return ErrorCode_Success;
    }

    void GLTSegment::close () {
        m_logtype_tables_manager.close();
    }

    epochtime_t GLTSegment::get_timestamp_at_offset(logtype_dictionary_id_t logtype_id, size_t offset) {
        if(!m_logtype_tables_manager.check_variable_column(logtype_id)) {
            m_logtype_tables_manager.load_variable_columns(logtype_id);
        }
        return m_logtype_tables_manager.get_timestamp_at_offset(logtype_id, offset);
    }

    void GLTSegment::get_variable_row_at_offset(logtype_dictionary_id_t logtype_id, size_t offset, GLTMessage& msg) {
        if(!m_logtype_tables_manager.check_variable_column(logtype_id)) {
            m_logtype_tables_manager.load_variable_columns(logtype_id);
        }
        m_logtype_tables_manager.get_variable_row_at_offset(logtype_id, offset, msg);
    }
}