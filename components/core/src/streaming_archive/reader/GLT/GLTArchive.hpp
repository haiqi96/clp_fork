#ifndef STREAMING_ARCHIVE_READER_GLT_ARCHIVE_HPP
#define STREAMING_ARCHIVE_READER_GLT_ARCHIVE_HPP
#include "../Archive.hpp"
#include "GLTFile.hpp"
#include "../../GLTMetadataDB.hpp"
#include "SingleLogtypeTableManager.hpp"

// Intended only for decompression.
namespace streaming_archive::reader::glt {
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
        bool get_next_message (GLTFile& file, GLTMessage& msg);

        // New functions

        // called upon opening the archive. figure out which segments are valid (i.e. non-0 size)
        void update_valid_segment_ids();
        std::vector<size_t> get_valid_segment () const {
            return m_valid_segment_id;
        };
        // read the filename.dict that maps id to filename
        void load_filename_dict();
        std::string get_file_name(file_id_t file_id) const;


        SingleLogtypeTableManager& get_table_manager () { return m_single_table_manager; }

        void open_table_manager(size_t segment_id);
        void close_table_manager();

        size_t decompress_messages_and_output(logtype_dictionary_id_t logtype_id, std::vector<epochtime_t>& ts, std::vector<file_id_t>& id,
                                              std::vector<encoded_variable_t>& vars, std::vector<bool>& wildcard_required, const Query& query);

        ErrorCode decompress_messages_and_send_result (logtype_dictionary_id_t logtype_id, std::vector<epochtime_t>& ts, std::vector<file_id_t>& id,
                                                      std::vector<encoded_variable_t>& vars, std::vector<bool>& wildcard_required,
                                                       const Query& query, const std::atomic_bool& query_cancelled, int controller_socket_fd);
        /**
         * Decompresses a given message using a fixed timestamp pattern
         * @param file
         * @param compressed_msg
         * @param decompressed_msg
         * @return true if message was successfully decompressed, false otherwise
         * @throw TimestampPattern::OperationFailed if failed to insert timestamp
         */
        bool decompress_message_with_fixed_timestamp_pattern (const Message& compressed_msg, std::string& decompressed_msg);


        /**
         * This functions assumes a specific logtype is loaded with m_variable_column_manager.
         * The function takes in all logtype_query associated with the logtype,
         * and finds next matching message in the 2D variable table
         *
         * @param logtype_query
         * @param msg
         * @param wildcard (by reference)
         * @param query (to provide time range info)
         * @return Return true if a matching message is found. wildcard gets set to true if the matching message
         *         still requires wildcard match
         * @throw Same as streaming_archive::reader::File::open_me
         */
        bool find_message_matching_with_logtype_query (const std::vector<LogtypeQuery>& logtype_query, GLTMessage& msg, bool& wildcard, const Query& query);

        /**
         * TBD
         * @param logtype_query
         * @param msg
         * @param wildcard
         * @param query
         * @param left
         * @param right
         * @return
         */
        bool find_message_matching_with_logtype_query_from_combined (const std::vector<LogtypeQuery>& logtype_query, GLTMessage& msg, bool& wildcard, const Query& query, size_t left, size_t right);

        /**
         * This functions assumes a specific logtype is loaded with m_variable_column_manager.
         * The function takes in all logtype_query associated with the logtype,
         * and finds next matching message in the 2D variable table
         *
         * @param logtype_query
         * @param msg
         * @param wildcard (by reference)
         * @param query (to provide time range info)
         * @return Return true if a matching message is found. wildcard gets set to true if the matching message
         *         still requires wildcard match
         * @throw Same as streaming_archive::reader::File::open_me
         */
        void find_message_matching_with_logtype_query_optimized (const std::vector<LogtypeQuery>& logtype_query, std::vector<bool>& wildcard, const Query& query, std::vector<size_t>& matched_row);

        /**
       * This functions assumes a specific logtype is loaded with m_variable_column_manager.
       * The function loads variable of the next message from the 2D variable table belonging to the specific logtype.
       * The variable are stored into the msg argument passed by reference
       *
       * @param msg
       * @return true if a row is successfully loaded into msg. false if the 2D table has reached the end
         */
        bool get_next_message_with_logtype (GLTMessage& msg);
    private:

        // This column manager is used for search only.
        SingleLogtypeTableManager m_single_table_manager;
        std::vector<std::string> m_filename_dict;
        std::vector<size_t> m_valid_segment_id;

        segment_id_t m_current_segment_id;
        GLTSegment m_segment;
        Segment m_message_order_table;
    };
}

#endif //STREAMING_ARCHIVE_READER_GLT_ARCHIVE_HPP
