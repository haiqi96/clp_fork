#ifndef STREAMING_ARCHIVE_WRITER_FILE_HPP
#define STREAMING_ARCHIVE_WRITER_FILE_HPP

// C++ standard libraries
#include <unordered_set>
#include <vector>

// Boost libraries
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

// Project headers
#include "../../Defs.h"
#include "../../ErrorCode.hpp"
#include "../../LogTypeDictionaryWriter.hpp"
#include "../../PageAllocatedVector.hpp"
#include "../../TimestampPattern.hpp"
#include "Segment.hpp"

namespace streaming_archive::writer {
    /**
     * Class representing a log file encoded in three columns - timestamps, logtype IDs, and variables.
     */
    class File {
    public:
        // Types
        class OperationFailed : public TraceableException {
        public:
            // Constructors
            OperationFailed (ErrorCode error_code, const char* const filename, int line_number) : TraceableException (error_code, filename, line_number) {}

            // Methods
            const char* what () const noexcept override {
                return "streaming_archive::writer::File operation failed";
            }
        };

        // Constructors
        File (const boost::uuids::uuid& id, const boost::uuids::uuid& orig_file_id, const std::string& orig_log_path, group_id_t group_id, size_t split_ix) :
                m_id(id),
                m_orig_file_id(orig_file_id),
                m_orig_log_path(orig_log_path),
                m_begin_ts(cEpochTimeMax),
                m_end_ts(cEpochTimeMin),
                m_group_id(group_id),
                m_num_uncompressed_bytes(0),
                m_num_messages(0),
                m_num_variables(0),
                m_segment_id(cInvalidSegmentId),
                m_is_split(split_ix > 0),
                m_split_ix(split_ix),
                m_is_open(false)
        {}

        // Destructor
        virtual ~File () = default;

        // Methods
        bool is_open () const { return m_is_open; }
        void open ();
        virtual void open_derived() = 0;
        void close () { m_is_open = false; }
        /**
         * Appends the file's columns to the given segment
         * @param logtype_dict
         * @param segment
         */
        virtual void append_to_segment (const LogTypeDictionaryWriter& logtype_dict,
                                        Segment& segment) = 0;

        /**
         * Changes timestamp pattern in use at current message in file
         * @param pattern
         */
        void change_ts_pattern (const TimestampPattern* pattern);

        /**
         * Returns whether the file contains any timestamp pattern
         * @return true if the file contains a timestamp pattern, false otherwise
         */
        bool has_ts_pattern () const { return m_timestamp_patterns.empty() == false; };

        /**
         * Gets the file's uncompressed size
         * @return File's uncompressed size in bytes
         */
        uint64_t get_num_uncompressed_bytes () const { return m_num_uncompressed_bytes; }

        /**
         * Gets the file's encoded size in bytes
         * @return Encoded size in bytes
         */
        size_t get_encoded_size_in_bytes () const {
            return m_num_messages * sizeof(epochtime_t) + m_num_messages * sizeof(logtype_dictionary_id_t) + m_num_variables * sizeof(encoded_variable_t);
        }

        /**
         * Gets the file's compression group ID
         * @return The compression group ID
         */
        group_id_t get_group_id () const { return m_group_id; };

        void set_is_split (bool is_split) { m_is_split = is_split; }

        /**
         * Gets file's original file path
         * @return file path
         */
        const std::string& get_orig_path () const { return m_orig_log_path; }
        const boost::uuids::uuid& get_orig_file_id () const { return m_orig_file_id; }
        std::string get_orig_file_id_as_string () const { return boost::uuids::to_string(m_orig_file_id); }
        const boost::uuids::uuid& get_id () const { return m_id; }
        std::string get_id_as_string () const { return boost::uuids::to_string(m_id); }
        epochtime_t get_begin_ts () const { return m_begin_ts; }
        epochtime_t get_end_ts () const { return m_end_ts; }
        const std::vector<std::pair<int64_t, TimestampPattern>>& get_timestamp_patterns () const { return m_timestamp_patterns; }
        std::string get_encoded_timestamp_patterns () const;
        uint64_t get_num_messages () const { return m_num_messages; }
        uint64_t get_num_variables () const { return m_num_variables; }

        segment_id_t get_segment_id () const { return m_segment_id; }
        bool is_split () const { return m_is_split; }
        size_t get_split_ix () const { return m_split_ix; }

    protected:
        // Types
        typedef enum {
            SegmentationState_NotInSegment = 0,
            SegmentationState_MovingToSegment,
            SegmentationState_InSegment
        } SegmentationState;

        // Variables
        // Metadata
        boost::uuids::uuid m_id;
        boost::uuids::uuid m_orig_file_id;

        std::string m_orig_log_path;

        epochtime_t m_begin_ts;
        epochtime_t m_end_ts;
        std::vector<std::pair<int64_t, TimestampPattern>> m_timestamp_patterns;

        group_id_t m_group_id;

        uint64_t m_num_uncompressed_bytes;

        uint64_t m_num_messages;
        uint64_t m_num_variables;

        segment_id_t m_segment_id;
        bool m_is_split;
        size_t m_split_ix;

        // State variables
        bool m_is_open;
    };
}

#endif // STREAMING_ARCHIVE_WRITER_FILE_HPP
