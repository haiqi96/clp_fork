#ifndef STREAMING_ARCHIVE_READER_CLPFILE_HPP
#define STREAMING_ARCHIVE_READER_CLPFILE_HPP

#include "../File.hpp"
#include "../SegmentManager.hpp"
namespace streaming_archive::reader::clp {
    class CLPFile : public File {
    public:
        CLPFile() : File(),
                m_segment_timestamps_decompressed_stream_pos(0),
                m_segment_logtypes_decompressed_stream_pos(0),
                m_segment_variables_decompressed_stream_pos(0),
                m_variables_ix(0),
                m_num_variables(0),
                m_num_segment_vars(0),
                m_variables(nullptr)
        {}

        ErrorCode open (const LogTypeDictionaryReader& archive_logtype_dict,
                        MetadataDB::FileIterator& file_metadata_ix,
                        SegmentManager& segment);

        void close_derived () override;
        void reset_indices () override;

        bool find_message_in_time_range (epochtime_t search_begin_timestamp,
                                         epochtime_t search_end_timestamp, Message& msg);
        const SubQuery* find_message_matching_query (const Query& query, Message& msg);
        bool get_next_message (Message& msg);

    private:
        uint64_t m_segment_timestamps_decompressed_stream_pos;
        uint64_t m_segment_logtypes_decompressed_stream_pos;
        uint64_t m_segment_variables_decompressed_stream_pos;
        std::unique_ptr<epochtime_t[]> m_segment_timestamps;
        std::unique_ptr<logtype_dictionary_id_t[]> m_segment_logtypes;
        std::unique_ptr<encoded_variable_t[]> m_segment_variables;

        size_t m_variables_ix;
        uint64_t m_num_variables;
        uint64_t m_num_segment_vars;

        logtype_dictionary_id_t* m_logtypes;
        epochtime_t* m_timestamps;
        encoded_variable_t* m_variables;
    };
}

#endif //CLPFILE_HPP
