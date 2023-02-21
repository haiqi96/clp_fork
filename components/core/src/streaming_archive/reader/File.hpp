#ifndef STREAMING_ARCHIVE_READER_FILE_HPP
#define STREAMING_ARCHIVE_READER_FILE_HPP

// C++ libraries
#include <list>
#include <set>
#include <vector>

// Project headers
#include "../../Defs.h"
#include "../../ErrorCode.hpp"
#include "../../LogTypeDictionaryReader.hpp"
#include "../../Query.hpp"
#include "../../TimestampPattern.hpp"
#include "../MetadataDB.hpp"
#include "Message.hpp"

namespace streaming_archive::reader {
    class File {
    public:
        // Types
        class OperationFailed : public TraceableException {
        public:
            // Constructors
            OperationFailed (ErrorCode error_code, const char* const filename, int line_number) :
                    TraceableException(error_code, filename, line_number) {}

            // Methods
            const char* what () const noexcept override {
                return "streaming_archive::reader::File operation failed";
            }
        };

        // Constructors
        File () :
                m_archive_logtype_dict(nullptr),
                m_begin_ts(cEpochTimeMax),
                m_end_ts(cEpochTimeMin),
                m_num_segment_msgs(0),
                m_msgs_ix(0),
                m_num_messages(0),
                m_current_ts_pattern_ix(0),
                m_current_ts_in_milli(0)
        {}

        // Methods
        /**
         * Opens file
         * @param archive_logtype_dict
         * @param file_metadata_ix
         * @return Same as SegmentManager::try_read
         * @return ErrorCode_Success on success
         */
        ErrorCode open (const LogTypeDictionaryReader& archive_logtype_dict, MetadataDB::FileIterator& file_metadata_ix);

        /**
         * Closes the file
         */
        void close ();

        // Methods
        const std::string& get_id_as_string() const { return m_id_as_string; }
        const std::string& get_orig_file_id_as_string() const { return m_orig_file_id_as_string; }
        epochtime_t get_begin_ts () const;
        epochtime_t get_end_ts () const;
        const std::string& get_orig_path () const;
        segment_id_t get_segment_id () const { return m_segment_id; }
        uint64_t get_num_messages () const { return m_num_messages; }
        bool is_split () const { return m_is_split; }

    protected:
        friend class Archive;

        virtual void close_derived() = 0;
        /**
         * Reset positions in columns
         */
        virtual void reset_indices ();

        const std::vector<std::pair<uint64_t, TimestampPattern>>& get_timestamp_patterns () const;
        epochtime_t get_current_ts_in_milli () const;
        size_t get_current_ts_pattern_ix () const;

        void increment_current_ts_pattern_ix ();

        // Variables
        const LogTypeDictionaryReader* m_archive_logtype_dict;

        epochtime_t m_begin_ts;
        epochtime_t m_end_ts;
        std::vector<std::pair<uint64_t, TimestampPattern>> m_timestamp_patterns;
        std::string m_id_as_string;
        std::string m_orig_file_id_as_string;
        std::string m_orig_path;

        segment_id_t m_segment_id;
        uint64_t m_num_segment_msgs;

        size_t m_msgs_ix;
        uint64_t m_num_messages;

        size_t m_current_ts_pattern_ix;
        epochtime_t m_current_ts_in_milli;

        size_t m_split_ix;
        bool m_is_split;
    };
}

#endif // STREAMING_ARCHIVE_READER_FILE_HPP