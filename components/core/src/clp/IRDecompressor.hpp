//
// Created by Haiqi on 2023/3/9.
//

#ifndef CLP_IRDECOMPRESSOR_HPP
#define CLP_IRDECOMPRESSOR_HPP

#include "../FileWriter.hpp"
#include "../Defs.h"
#include "../ffi/ir_stream/protocol_constants.hpp"
#include "../streaming_archive/reader/IRMessage.hpp"

namespace clp {
    class IRDecompressor {
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
        void close ();

        bool write_premable(epochtime_t reference_ts,
                            const std::string& timestamp_pattern,
                            const std::string& timestamp_pattern_syntax,
                            const std::string& timezone);

        void write_msg(const streaming_archive::reader::IRMessage& ir_msg);

    private:
        // Variables
        void write_encoded_var(encoded_variable_t encoded_var);
        void write_dict_var(const std::string& dict_var);

        void write_logtype(const std::string& logtype_str);
        void write_timestamp(epochtime_t ts);

        void write_null_ts_tag();

        template <typename integer_t> void encode_int (integer_t value);

        void write_magic_number();
        FileWriter m_decompressed_file_writer;
        epochtime_t last_ts;
    };
}

#endif //CLP_IRDECOMPRESSOR_HPP
