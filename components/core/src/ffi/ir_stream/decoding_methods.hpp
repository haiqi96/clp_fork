#ifndef FFI_IR_STREAM_DECODING_METHODS_HPP
#define FFI_IR_STREAM_DECODING_METHODS_HPP

// C++ standard libraries
#include <string_view>
#include <vector>

// Project headers
#include "../encoding_methods.hpp"
#include "../../BufferedReaderInterface.hpp"
namespace ffi::ir_stream {

    using encoded_tag_t = uint8_t;
    typedef enum {
        IRErrorCode_Success,
        IRErrorCode_Decode_Error,
        IRErrorCode_Eof,
        IRErrorCode_Corrupted_IR,
        IRErrorCode_Incomplete_IR,
    } IRErrorCode;

    /**
     * Decodes the encoding type for the encoded IR stream
     * @param ir_buf
     * @param is_four_bytes_encoding Returns the encoding type
     * @return ErrorCode_Success on success
     * @return ErrorCode_Corrupted_IR if ir_buf contains invalid IR
     * @return ErrorCode_Incomplete_IR if ir_buf doesn't contain enough data to
     * decode
     */
    IRErrorCode get_encoding_type (BufferedReaderInterface& ir_buf, bool& is_four_bytes_encoding);

    /**
     * Decodes the preamble for an IR stream.
     * @param ir_buf
     * @param metadata_type Returns the type of the metadata found in the IR
     * @param metadata_pos Returns the starting position of the metadata in ir_buf
     * @param metadata_size Returns the size of the metadata written in the IR
     * @return IRErrorCode_Success on success
     * @return IRErrorCode_Corrupted_IR if ir_buf contains invalid IR
     * @return IRErrorCode_Incomplete_IR if ir_buf doesn't contain enough
     * data to decode
     */
    IRErrorCode decode_preamble (BufferedReaderInterface& ir_buf, encoded_tag_t& metadata_type,
                                 size_t& metadata_pos, uint16_t& metadata_size);

    namespace eight_byte_encoding {
        /**
         * Decodes the next message for the eight-byte encoding IR stream.
         * @param ir_buf
         * @param message Returns the decoded message
         * @param timestamp Returns the decoded timestamp
         * @return ErrorCode_Success on success
         * @return ErrorCode_Corrupted_IR if ir_buf contains invalid IR
         * @return ErrorCode_Decode_Error if the encoded message cannot be
         * properly decoded
         * @return ErrorCode_Incomplete_IR if ir_buf doesn't contain enough data
         * to decode
         * @return ErrorCode_End_of_IR if the IR ends
         */
        IRErrorCode decode_next_message (BufferedReaderInterface& ir_buf, std::string& message,
                                         epoch_time_ms_t& timestamp);
    }

    namespace four_byte_encoding {
        /**
         * Decodes the next message for the four-byte encoding IR stream.
         * @param ir_buf
         * @param message Returns the decoded message
         * @param timestamp_delta Returns the decoded timestamp delta
         * @return ErrorCode_Success on success
         * @return ErrorCode_Corrupted_IR if ir_buf contains invalid IR
         * @return ErrorCode_Decode_Error if the encoded message cannot be
         * properly decoded
         * @return ErrorCode_Incomplete_IR if ir_buf doesn't contain enough data
         * to decode
         * @return ErrorCode_End_of_IR if the IR ends
         */
        IRErrorCode decode_next_message (BufferedReaderInterface& ir_buf, std::string& message,
                                         epoch_time_ms_t& timestamp_delta);
    }
}

#endif //FFI_IR_STREAM_DECODING_METHODS_HPP
