#include "CLPArchive.hpp"

using streaming_archive::clp::CLPMetadataDB;

namespace streaming_archive::reader::clp {
    void CLPArchive::open_derived (const std::string& path) {

        m_metadata_db = std::make_unique<CLPMetadataDB>();
        auto metadata_db_path = boost::filesystem::path(path) / cMetadataDBFileName;
        m_metadata_db->open(metadata_db_path.string());

        m_segment_manager.open(m_segments_dir_path);
    }

    void CLPArchive::close_derived () {
        m_segment_manager.close();
        m_metadata_db->close();
    }

    ErrorCode CLPArchive::open_file (CLPFile& file, MetadataDB::FileIterator& file_metadata_ix) {
        return file.open(m_logtype_dictionary, file_metadata_ix, m_segment_manager);
    }

    void CLPArchive::close_file (CLPFile& file) {
        file.close();
    }

    void CLPArchive::reset_file_indices (CLPFile& file) {
        file.reset_indices();
    }

    bool CLPArchive::find_message_in_time_range (CLPFile& file, epochtime_t search_begin_timestamp, epochtime_t search_end_timestamp, Message& msg) {
        return file.find_message_in_time_range(search_begin_timestamp, search_end_timestamp, msg);
    }

    const SubQuery* CLPArchive::find_message_matching_query (CLPFile& file, const Query& query, Message& msg) {
        return file.find_message_matching_query(query, msg);
    }

    bool CLPArchive::get_next_message (CLPFile& file, Message& msg) {
        return file.get_next_message(msg);
    }
}