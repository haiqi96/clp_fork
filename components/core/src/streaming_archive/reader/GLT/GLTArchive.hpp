#ifndef STREAMING_ARCHIVE_READER_GLT_ARCHIVE_HPP
#define STREAMING_ARCHIVE_READER_GLT_ARCHIVE_HPP
#include "../Archive.hpp"
#include "GLTFile.hpp"
#include "../../MetadataDB.hpp"

// Intended only for decompression.
namespace streaming_archive::reader {
    class GLTArchive : public Archive {
    public:

        GLTArchive() : Archive(), m_current_segment_id(INT64_MAX) {}

        void open_derived(const std::string &path) override;
        void close_derived() override;

        /**
         * Opens file with given path
         * @param file
         * @param file_metadata_ix
         * @param read_ahead Whether to read-ahead in the file (if possible)
         * @return Same as streaming_archive::reader::File::open_me
         * @throw Same as streaming_archive::reader::File::open_me
         */
        ErrorCode open_file (GLTFile& file, MetadataDB::FileIterator& file_metadata_ix);

        /**
         * Wrapper for streaming_archive::reader::File::close_me
         * @param file
         */
        void close_file (GLTFile& file);
        /**
         * Wrapper for streaming_archive::reader::File::reset_indices
         * @param file
         */
        void reset_file_indices (GLTFile& file);

        /**
         * Wrapper for streaming_archive::reader::File::get_next_message
         */
        bool get_next_message (GLTFile& file, Message& msg);

    private:

        segment_id_t m_current_segment_id;
        GLTSegment m_segment;
        Segment m_message_order_table;
    };
}

#endif //STREAMING_ARCHIVE_READER_ARCHIVE_HPP
