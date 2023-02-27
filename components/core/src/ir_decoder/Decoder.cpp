#include "Decoder.hpp"

#include "utils.hpp"
#include <iostream>

namespace ir_decoder {
    bool Decoder::decode (std::string input_path) {

        m_file_reader.open(input_path);
        // Check that file is CLP encoded
        auto error_code = m_file_reader.try_read(m_clp_custom_encoding_buf, cCLPMagicNumberBufCapacity, m_clp_custom_buf_length);
        if (ErrorCode_Success != error_code) {
            if (ErrorCode_EndOfFile != error_code) {
                SPDLOG_ERROR("Failed to read {}, errno={}", input_path.c_str(), errno);
                return false;
            }
        }

        bool succeeded = true;
        bool is_compacted_encoding = false;
        if (is_clp_magic_number(m_clp_custom_buf_length, m_clp_custom_encoding_buf, is_compacted_encoding)) {
            parse_and_decode(m_file_reader, is_compacted_encoding);
        } else {
            // this should be try compressing as zstd
            SPDLOG_ERROR("Unexpected input for now, will implement zstd later");
            return false;
        }

        m_file_reader.close();

        return succeeded;
    }

    void Decoder::parse_and_decode (ReaderInterface& reader, bool is_compact_encoding)
    {
        m_encoded_parsed_message.clear();
        // ToDo: Open compressed file

        // Parse content from metadat
        if(!m_encoded_message_parser.parse_metadata(reader, m_encoded_parsed_message, is_compact_encoding)){
            SPDLOG_ERROR("Corrupted metadata.");
        }
        // we don't parse the validation buffer anymore because it only contains the magic number
        /* was parsing the validation buffer */

        if(false == is_compact_encoding) {
            while (m_encoded_message_parser.parse_next_std_token(reader, m_encoded_parsed_message)) {
                std::string recovered_string = m_encoded_parsed_message.recover_message();
                std::cout << recovered_string;
            }
        } else {
            while (m_encoded_message_parser.parse_next_compact_token(reader, m_encoded_parsed_message)) {
                std::string recovered_string = m_encoded_parsed_message.recover_message();
                std::cout << recovered_string;
            }
        }
    }
}