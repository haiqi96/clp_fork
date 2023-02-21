//
// Created by haiqi on 2023/1/21.
//

#ifndef STREAMING_ARCHIVE_READER_GLT_FILE_HPP
#define STREAMING_ARCHIVE_READER_GLT_FILE_HPP

#include "../File.hpp"
#include "GLTSegment.hpp"

namespace streaming_archive::reader::glt {
    class GLTFile : public File {
    public:
        GLTFile () : File(),
                m_logtypes_fd(-1),
                m_logtypes_file_size(0),
                m_logtypes(nullptr),
                m_offsets_fd(-1),
                m_offsets_file_size(0),
                m_segment_logtypes_decompressed_stream_pos(0),
                m_segment(nullptr),
                m_offsets(nullptr)
        {}

        ErrorCode open (const LogTypeDictionaryReader& archive_logtype_dict,
                        MetadataDB::FileIterator& file_metadata_ix,
                        GLTSegment& segment,
                        Segment& message_order_table);
        void close_derived () override;

        /**
         * Get next message in file
         * @param msg
         * @return true if message read, false if no more messages left
         */
        bool get_next_message (GLTMessage& msg);

        /**
         * Get logtype table offset of the logtype_id
         * @param logtype_id
         * @param msg_ix
         * @return offset of the message
         */
        size_t get_msg_offset(logtype_dictionary_id_t logtype_id, size_t msg_ix);

    private:
        friend class GLTArchive;
        uint64_t m_segment_logtypes_decompressed_stream_pos;
        uint64_t m_segment_offsets_decompressed_stream_pos;
        std::unique_ptr<logtype_dictionary_id_t[]> m_segment_logtypes;
        std::unique_ptr<size_t[]> m_segment_offsets;

        GLTSegment* m_segment;

        int m_logtypes_fd;
        size_t m_logtypes_file_size;
        logtype_dictionary_id_t* m_logtypes;

        int m_offsets_fd;
        size_t m_offsets_file_size;
        size_t* m_offsets;

        // for keeping the logtype table's offset
        std::unordered_map<logtype_dictionary_id_t, size_t> m_logtype_table_offsets;
    };
}

#endif //STREAMING_ARCHIVE_READER_GLT_FILE_HPP
