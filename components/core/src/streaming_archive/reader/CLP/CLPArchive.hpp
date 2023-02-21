#ifndef STREAMING_ARCHIVE_READER_CLPARCHIVE_HPP
#define STREAMING_ARCHIVE_READER_CLPARCHIVE_HPP

#include "../../CLPMetadataDB.hpp"
#include "../Archive.hpp"
#include "../SegmentManager.hpp"
#include "CLPFile.hpp"

namespace streaming_archive::reader::clp {
    class CLPArchive : public Archive {
    public:
        void open_derived(const std::string &path) override;
        void close_derived() override;
        /**
         * Opens file with given path
         * @param file
         * @param file_metadata_ix
         * @return Same as streaming_archive::reader::clp::CLPFile::open
         * @throw Same as streaming_archive::reader::clp::CLPFile::open
         */
        ErrorCode open_file (CLPFile& file, MetadataDB::FileIterator& file_metadata_ix);
        /**
         * Wrapper for streaming_archive::reader::File::close_me
         * @param file
         */
        void close_file (CLPFile& file);
        /**
         * Wrapper for streaming_archive::reader::File::reset_indices
         * @param file
         */
        void reset_file_indices (CLPFile& file);

        /**
         * Wrapper for streaming_archive::reader::clp::CLPFile::find_message_in_time_range
         */
        bool find_message_in_time_range (CLPFile& file, epochtime_t search_begin_timestamp, epochtime_t search_end_timestamp, Message& msg);
        /**
         * Wrapper for streaming_archive::reader::clp::CLPFile::find_message_matching_query
         */
        const SubQuery* find_message_matching_query (CLPFile& file, const Query& query, Message& msg);
        /**
         * Wrapper for streaming_archive::reader::clp::CLPFile::get_next_message
         */
        bool get_next_message (CLPFile& file, Message& msg);
    private:
        SegmentManager m_segment_manager;
    };
}

#endif //CLPARCHIVE_HPP
