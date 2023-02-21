#ifndef STREAMING_ARCHIVE_READER_ARCHIVE_HPP
#define STREAMING_ARCHIVE_READER_ARCHIVE_HPP

// C++ libraries
#include <filesystem>
#include <iterator>
#include <list>
#include <memory>
#include <string>
#include <utility>

// Boost libraries
#include <boost/filesystem.hpp>

// Project headers
#include "../../ErrorCode.hpp"
#include "../../LogTypeDictionaryReader.hpp"
#include "../../Query.hpp"
#include "../../SQLiteDB.hpp"
#include "../../VariableDictionaryReader.hpp"
#include "../MetadataDB.hpp"
#include "File.hpp"
#include "Message.hpp"

namespace streaming_archive::reader {
    class Archive {
    public:
        // Types
        class OperationFailed : public TraceableException {
        public:
            // Constructors
            OperationFailed (ErrorCode error_code, const char* const filename, int line_number) : TraceableException (error_code, filename, line_number) {}

            // Methods
            const char* what () const noexcept override {
                return "streaming_archive::reader::Archive operation failed";
            }
        };

        // Methods
        /**
         * Opens archive for reading
         * @param path
         * @throw streaming_archive::reader::Archive::OperationFailed if could not stat file or it isn't a directory or metadata is corrupted
         * @throw FileReader::OperationFailed if failed to open any dictionary
         */
        void open (const std::string& path);
        void close ();

        virtual void open_derived (const std::string& path) = 0;
        virtual void close_derived () = 0;

        /**
         * Reads any new entries added to the dictionaries
         * @throw Same as LogTypeDictionary::read_from_file and VariableDictionary::read_from_file
         */
        void refresh_dictionaries ();
        const LogTypeDictionaryReader& get_logtype_dictionary () const;
        const VariableDictionaryReader& get_var_dictionary () const;

        /**
         * Decompresses a given message from a given file
         * @param file
         * @param compressed_msg
         * @param decompressed_msg
         * @return true if message was successfully decompressed, false otherwise
         * @throw TimestampPattern::OperationFailed if failed to insert timestamp
         */
        bool decompress_message (File& file, const Message& compressed_msg, std::string& decompressed_msg);

        void decompress_empty_directories (const std::string& output_dir);

        std::unique_ptr<MetadataDB::FileIterator> get_file_iterator () {
            return m_metadata_db->get_file_iterator(cEpochTimeMin, cEpochTimeMax, "", false, cInvalidSegmentId);
        }
        std::unique_ptr<MetadataDB::FileIterator> get_file_iterator (const std::string& file_path) {
            return m_metadata_db->get_file_iterator(cEpochTimeMin, cEpochTimeMax, file_path, false, cInvalidSegmentId);
        }
        std::unique_ptr<MetadataDB::FileIterator> get_file_iterator (epochtime_t begin_ts, epochtime_t end_ts, const std::string& file_path) {
            return m_metadata_db->get_file_iterator(begin_ts, end_ts, file_path, false, cInvalidSegmentId);
        }
        std::unique_ptr<MetadataDB::FileIterator> get_file_iterator (epochtime_t begin_ts, epochtime_t end_ts, const std::string& file_path,
                                                                     segment_id_t segment_id)
        {
            return m_metadata_db->get_file_iterator(begin_ts, end_ts, file_path, true, segment_id);
        }

    protected:
        // Variables
        std::string m_id;
        std::string m_path;
        std::string m_segments_dir_path;
        LogTypeDictionaryReader m_logtype_dictionary;
        VariableDictionaryReader m_var_dictionary;

        std::unique_ptr<MetadataDB> m_metadata_db;
    };
}

#endif // STREAMING_ARCHIVE_READER_ARCHIVE_HPP