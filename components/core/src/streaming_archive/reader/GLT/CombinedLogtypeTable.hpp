#ifndef STREAMING_ARCHIVE_READER_GLT_COMBINEDLOGTYPETABLES_HPP
#define STREAMING_ARCHIVE_READER_GLT_COMBINEDLOGTYPETABLES_HPP

// C++ libraries
#include <vector>

// spdlog
#include <spdlog/spdlog.h>

// Project headers
#include "../../../Defs.h"
#include "../../../ErrorCode.hpp"
#include "../../../streaming_compression/passthrough/Decompressor.hpp"
#include "../../../streaming_compression/zstd/Decompressor.hpp"
#include "GLTMessage.hpp"
#include "LogtypeMetadata.hpp"

namespace streaming_archive::reader::glt {
    class CombinedLogtypeTable {
    public:

        // Types
        class OperationFailed : public TraceableException {
        public:
            // Constructors
            OperationFailed (ErrorCode error_code, const char* const filename, int line_number) : TraceableException (error_code, filename, line_number) {}

            // Methods
            const char* what () const noexcept override {
                return "CombinedLogtypeTables operation failed";
            }
        };

        CombinedLogtypeTable ();

        // open a logtype table, load from it, and also get the information of logtype->metadata
        // later we might want to find a smarter way to pass the 3rd argument or do some preprocessing
        void open (combined_table_id_t table_id);
        void close ();

        void open_logtype_table (logtype_dictionary_id_t logtype_id,
                                 streaming_compression::Decompressor& decompressor,
                                 const std::unordered_map<logtype_dictionary_id_t, CombinedMetadata>& metadata);

        void open_and_read_once_only (logtype_dictionary_id_t logtype_id,
                                      combined_table_id_t combined_table_id,
                                      streaming_compression::Decompressor& decompressor,
                                      const std::unordered_map<logtype_dictionary_id_t, CombinedMetadata>& metadata);

        void close_logtype_table ();

        epochtime_t get_timestamp_at_offset (size_t offset);
        void get_row_at_offset (size_t offset, GLTMessage& msg);
        bool get_next_full_row (GLTMessage& msg);

        bool get_next_message_partial (GLTMessage& msg, size_t l, size_t r);
        void skip_next_row ();
        void get_remaining_message (GLTMessage& msg, size_t l, size_t r);

        bool is_open() const { return m_is_open; }
        bool is_logtype_table_open() const { return m_is_logtype_open; }

    private:

        void load_logtype_table_data (streaming_compression::Decompressor& decompressor, char* read_buffer);

        combined_table_id_t m_table_id;
        logtype_dictionary_id_t m_logtype_id;
        size_t m_current_row;
        size_t m_num_row;
        size_t m_num_columns;

        bool m_is_open;
        bool m_is_logtype_open;
        // question: do we still need a malloced buffer?
        std::unique_ptr<char[]> m_read_buffer;
        size_t m_buffer_size;
        // for this data structure, m_column_based_variables[i] means all data at i th column
        // m_column_based_variables[i][j] means j th row at the i th column
        std::vector<encoded_variable_t> m_column_based_variables;
        std::vector<bool> m_column_loaded;
        std::vector<encoded_variable_t> m_timestamps;
        std::vector<file_id_t> m_file_ids;
    };
}

#endif //STREAMING_ARCHIVE_READER_GLT_COMBINEDLOGTYPETABLES_HPP