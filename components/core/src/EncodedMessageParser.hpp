//
// Created by haiqixu on 1/30/2022.
//

#ifndef CLP_ENCODEDMASSAGEPARSER_HPP
#define CLP_ENCODEDMASSAGEPARSER_HPP


// C++ standard libraries
#include <string>

// Project headers
#include "ErrorCode.hpp"
#include "ParsedMessage.hpp"
#include "ReaderInterface.hpp"
#include "TraceableException.hpp"
#include "EncodedParsedMessage.hpp"
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

/**
 * Class to parse log messages
 */
class EncodedMessageParser {
public:
    // Types
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed (ErrorCode error_code, const char* const filename, int line_number) : TraceableException (error_code, filename, line_number) {}

        // Methods
        const char* what () const noexcept override {
            return "EncodedMessageParser operation failed";
        }
    };

    // Methods
    /**
     * Parses the next message from the given buffer. Messages are delimited either by i) a timestamp or ii) a line break if no timestamp is found.
     * @param drain_source Whether to drain all content from the file or just lines with endings
     * @param buffer_length
     * @param buffer
     * @param buf_pos
     * @param message
     * @return true if message parsed, false otherwise
     */
    bool parse_metadata (ReaderInterface& reader, EncodedParsedMessage& message, bool is_compact_encoding);

    bool parse_next_token(ReaderInterface& reader, EncodedParsedMessage& message);

private:
    // Methods
    /**
     * Parses the line and adds it either to the buffered message if incomplete, or the given message if complete
     * @param message
     * @return Whether a complete message has been parsed
     */
    unsigned char read_byte (ReaderInterface &reader);
    unsigned short read_short (ReaderInterface &reader);
    unsigned int read_unsigned (ReaderInterface &reader);
    unsigned long long read_long (ReaderInterface &reader);

    bool parse_next_std_token (ReaderInterface& reader, EncodedParsedMessage& message);
    bool parse_next_compact_token (ReaderInterface& reader, EncodedParsedMessage& message);

    void parse_unencoded_vars (ReaderInterface& reader, EncodedParsedMessage& message, unsigned char tag_byte);
    void parse_log_message(ReaderInterface& reader, EncodedParsedMessage& message, unsigned char tag_byte);

    // variables
    std::string m_timezone;
    epochtime_t m_last_timestamp;
    std::string m_version;
    bool m_compact_encoding;
};

#endif //CLP_ENCODEDMASSAGEPARSER_HPP
