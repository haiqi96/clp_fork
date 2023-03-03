#ifndef STREAMING_ARCHIVE_CLP_ARCHIVE_HPP
#define STREAMING_ARCHIVE_CLP_ARCHIVE_HPP

// Project headers
#include "../../../compressor_frontend/Constants.hpp"
#include "../Archive.hpp"
#include "CLPFile.hpp"

namespace streaming_archive::writer {
    class CLPArchive : public Archive {
    public:

        // Constructors
        CLPArchive ();

        // Methods
        void open_derived(const UserConfig& user_config) override {}
        void close_derived () override;

        void create_and_open_file (const std::string& path, group_id_t group_id,
                                   const boost::uuids::uuid& orig_file_id,
                                   size_t split_ix) override;

        void write_msg (epochtime_t timestamp, const std::string& message,
                        size_t num_uncompressed_bytes) override;

        void write_msg_using_schema (compressor_frontend::Token*& uncompressed_msg,
                                     uint32_t uncompressed_msg_pos, bool has_delimiter,
                                     bool has_timestamp) override;

        void append_file_to_segment () override;
        /**
         * Appends the content of the current encoded file to the given segment
         * @param segment
         * @param logtype_ids_in_segment
         * @param var_ids_in_segment
         * @param files_in_segment
         */
        void append_file_contents_to_segment (Segment& clp_segment,
                                              ArrayBackedPosIntSet<logtype_dictionary_id_t>& logtype_ids_in_segment,
                                              ArrayBackedPosIntSet<variable_dictionary_id_t>& var_ids_in_segment,
                                              std::vector<File*>& files_in_segment);

        /**
         * Closes a given segment, persists the metadata of the files in the segment, and cleans up any data remaining outside the segment
         * @param segment
         * @param files
         * @param segment_logtype_ids
         * @param segment_var_ids
         * @throw Same as streaming_archive::writer::CompressedStreamOnDisk::close
         * @throw Same as streaming_archive::writer::Archive::persist_file_metadata
         */
        void close_segment_and_persist_file_metadata (Segment& clp_segment,
                                                      std::vector<File*>& files,
                                                      ArrayBackedPosIntSet<logtype_dictionary_id_t>& segment_logtype_ids,
                                                      ArrayBackedPosIntSet<variable_dictionary_id_t>& segment_var_ids);

    private:
        Segment m_clp_segment;
        CLPFile* m_clp_file;
    };
};
#endif //STREAMING_ARCHIVE_CLP_ARCHIVE_HPP