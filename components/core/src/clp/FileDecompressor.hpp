#ifndef CLP_FILEDECOMPRESSOR_HPP
#define CLP_FILEDECOMPRESSOR_HPP

// C++ standard libraries
#include <string>

// Project headers
#include "../FileWriter.hpp"
#include "../streaming_archive/MetadataDB.hpp"
#include "../streaming_archive/reader/CLP/CLPArchive.hpp"
#include "../streaming_archive/reader/File.hpp"
#include "../streaming_archive/reader/Message.hpp"

namespace clp {
    /**
     * Class to hold the data structures that are used to decompress files rather than recreating them within the decompression function or passing them as
     * parameters.
     */
    class FileDecompressor {
    public:
        // Methods
        bool decompress_file (streaming_archive::MetadataDB::FileIterator& file_metadata_ix, const std::string& output_dir,
                              streaming_archive::reader::clp::CLPArchive& archive_reader, std::unordered_map<std::string, std::string>& temp_path_to_final_path);

    private:
        // Variables
        FileWriter m_decompressed_file_writer;
        streaming_archive::reader::clp::CLPFile m_encoded_file;
        streaming_archive::reader::Message m_encoded_message;
        std::string m_decompressed_message;
    };
};

#endif // CLP_FILEDECOMPRESSOR_HPP