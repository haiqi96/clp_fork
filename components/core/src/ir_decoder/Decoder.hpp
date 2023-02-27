//
// Created by haiqi on 2023/2/26.
//

#ifndef IR_DECODER_DECODER_HPP
#define IR_DECODER_DECODER_HPP

#include "../FileReader.hpp"
#include "../EncodedMessageParser.hpp"
#include "../EncodedParsedMessage.hpp"

namespace ir_decoder {
    constexpr size_t cCLPMagicNumberBufCapacity = 4;
    class Decoder {
    public:
        bool decode(std::string input_path);
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

        FileReader m_file_reader;
        EncodedMessageParser m_encoded_message_parser;
        EncodedParsedMessage m_encoded_parsed_message;
        size_t m_clp_custom_buf_length;
        char m_clp_custom_encoding_buf[cCLPMagicNumberBufCapacity];
    };
}

#endif //IR_DECODER_DECODER_HPP
