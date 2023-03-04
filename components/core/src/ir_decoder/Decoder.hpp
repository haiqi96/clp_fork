//
// Created by haiqi on 2023/2/26.
//

#ifndef IR_DECODER_DECODER_HPP
#define IR_DECODER_DECODER_HPP

#include "../FileReader.hpp"
#include "../IRMessageParser.hpp"
#include "../ParsedIRMessage.hpp"
#include "../LibarchiveFileReader.hpp"
#include "../LibarchiveReader.hpp"
#include "../streaming_compression/zstd/Decompressor.hpp"
#include "../ffi/ir_stream/protocol_constants.hpp"
#include "../type_utils.hpp"

namespace ir_decoder {
    constexpr size_t cArchiveValidationBufCapacity = 4096;
    constexpr size_t cIRValidationBufCapacity = ffi::ir_stream::cProtocol::MagicNumberLength;
    class Decoder {
    public:
        bool decode(std::string input_path, std::string output_path);
    private:
        // Methods
        /**
         * Parses and encodes content from the given reader into the given archive_writer
         * @param target_data_size_of_dicts
         * @param archive_user_config
         * @param target_encoded_file_size
         * @param path_for_compression
         * @param group_id
         * @param archive_writer
         * @param reader
         */
        void parse_and_decode (ReaderInterface& reader, bool is_compact_encoding);

        bool try_compressing_as_archive(std::string input_path);

        FileReader m_file_reader;
        FileWriter m_file_writer;

        LibarchiveReader m_libarchive_reader;
        LibarchiveFileReader m_libarchive_file_reader;
        IRMessageParser m_encoded_message_parser;
        ParsedIRMessage m_encoded_parsed_message;
        size_t m_ir_validation_buf_length;
        char m_ir_validation_buf[cIRValidationBufCapacity];
        size_t m_validation_buf_length;
        char m_archive_validation_buf[cArchiveValidationBufCapacity];
        streaming_compression::zstd::Decompressor m_zstd_decompressor;
    };
}

#endif //IR_DECODER_DECODER_HPP
