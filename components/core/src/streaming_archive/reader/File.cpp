#include "File.hpp"

// C libraries
#include <sys/stat.h>
#include <unistd.h>

// Project headers
#include "../../EncodedVariableInterpreter.hpp"
#include "../../spdlog_with_specializations.hpp"
#include "../Constants.hpp"

using namespace std;

namespace streaming_archive::reader {
    epochtime_t File::get_begin_ts () const {
        return m_begin_ts;
    }
    epochtime_t File::get_end_ts () const {
        return m_end_ts;
    }

    ErrorCode File::open (const LogTypeDictionaryReader& archive_logtype_dict, MetadataDB::FileIterator& file_metadata_ix)
    {
        m_archive_logtype_dict = &archive_logtype_dict;

        // Populate metadata from database document
        file_metadata_ix.get_id(m_id_as_string);
        file_metadata_ix.get_orig_file_id(m_orig_file_id_as_string);
        file_metadata_ix.get_path(m_orig_path);
        m_begin_ts = file_metadata_ix.get_begin_ts();
        m_end_ts = file_metadata_ix.get_end_ts();

        string encoded_timestamp_patterns;
        file_metadata_ix.get_timestamp_patterns(encoded_timestamp_patterns);
        size_t begin_pos = 0;
        size_t end_pos;
        string timestamp_format;
        while (true) {
            end_pos = encoded_timestamp_patterns.find_first_of(':', begin_pos);
            if (string::npos == end_pos) {
                // Done
                break;
            }
            size_t msg_num = strtoull(&encoded_timestamp_patterns[begin_pos], nullptr, 10);
            begin_pos = end_pos + 1;

            end_pos = encoded_timestamp_patterns.find_first_of(':', begin_pos);
            if (string::npos == end_pos) {
                // Unexpected truncation
                throw OperationFailed(ErrorCode_Corrupt, __FILENAME__, __LINE__);
            }
            uint8_t num_spaces_before_ts = strtol(&encoded_timestamp_patterns[begin_pos], nullptr,
                                                  10);
            begin_pos = end_pos + 1;

            end_pos = encoded_timestamp_patterns.find_first_of('\n', begin_pos);
            if (string::npos == end_pos) {
                // Unexpected truncation
                throw OperationFailed(ErrorCode_Corrupt, __FILENAME__, __LINE__);
            }
            timestamp_format.assign(encoded_timestamp_patterns, begin_pos, end_pos - begin_pos);
            begin_pos = end_pos + 1;

            m_timestamp_patterns.emplace_back(
                    std::piecewise_construct,
                    std::forward_as_tuple(msg_num),
                    forward_as_tuple(num_spaces_before_ts, timestamp_format));
        }

        m_num_messages = file_metadata_ix.get_num_messages();
        m_segment_id = file_metadata_ix.get_segment_id();

        m_is_split = file_metadata_ix.is_split();
        m_split_ix = file_metadata_ix.get_split_ix();

        m_msgs_ix = 0;

        m_current_ts_pattern_ix = 0;
        m_current_ts_in_milli = m_begin_ts;

        return ErrorCode_Success;
    }

    void File::close () {

        close_derived();

        m_msgs_ix = 0;
        m_num_messages = 0;

        m_current_ts_pattern_ix = 0;
        m_current_ts_in_milli = 0;
        m_timestamp_patterns.clear();

        m_begin_ts = cEpochTimeMax;
        m_end_ts = cEpochTimeMin;
        m_orig_path.clear();

        m_archive_logtype_dict = nullptr;
    }

    void File::reset_indices () {
        m_msgs_ix = 0;
    }

    const string& File::get_orig_path () const {
        return m_orig_path;
    }

    const vector<pair<uint64_t, TimestampPattern>>& File::get_timestamp_patterns () const {
        return m_timestamp_patterns;
    }

    epochtime_t File::get_current_ts_in_milli () const {
        return m_current_ts_in_milli;
    }
    size_t File::get_current_ts_pattern_ix () const {
        return m_current_ts_pattern_ix;
    }

    void File::increment_current_ts_pattern_ix () {
        ++m_current_ts_pattern_ix;
    }
}