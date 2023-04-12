//
// Created by Haiqi on 2023/3/9.
//

#ifndef CLP_STDIRDECOMPRESSOR_HPP
#define CLP_STDIRDECOMPRESSOR_HPP

#include "../FileWriter.hpp"
#include "../Defs.h"
#include "../ffi/ir_stream/protocol_constants.hpp"
#include "../streaming_archive/reader/IRMessage.hpp"
#include "../streaming_compression/zstd/Compressor.hpp"

namespace clp {
    class STDIRDecompressor {
    public:
        // Methods
        /**
         * Opens a file for writing
         * @param path
         * @param open_mode The mode to open the file with
         * @throw FileWriter::OperationFailed on failure
         */
        void open (const std::string& path, FileWriter::OpenMode open_mode);
        /**
         * Closes the file
         * @throw FileWriter::OperationFailed on failure
         */
        void close_without_eof ();

        bool write_premable(const std::string& timestamp_pattern,
                            const std::string& timestamp_pattern_syntax,
                            const std::string& timezone);

        void write_msg (const streaming_archive::reader::IRMessage& ir_msg);

        void write_eof_and_close();

    private:
        // Variables
        void write_encoded_var(encoded_variable_t encoded_var);
        void write_dict_var(const std::string& dict_var);

        void write_logtype(const std::string& logtype_str);
        void write_timestamp(epochtime_t ts);

        template <typename integer_t> void encode_int (integer_t value);

        streaming_compression::zstd::Compressor m_zstd_ir_compressor;
        FileWriter m_decompressed_file_writer;
    };
}

#endif //CLP_STDIRDECOMPRESSOR_HPP
