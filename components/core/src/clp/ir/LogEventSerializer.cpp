#include "LogEventSerializer.hpp"

#include <string_utils/string_utils.hpp>

#include "../ffi/ir_stream/encoding_methods.hpp"
#include "../ffi/ir_stream/protocol_constants.hpp"
#include "spdlog_with_specializations.hpp"

using std::string;
using std::string_view;

namespace clp::ir {
template <typename encoded_variable_t>
LogEventSerializer<encoded_variable_t>::~LogEventSerializer() {
    if (true == m_is_open) {
        SPDLOG_ERROR("Serializer is not closed before being destroyed - output maybe corrupted");
    }
}

template <typename encoded_variable_t>
auto LogEventSerializer<encoded_variable_t>::open(
        string const& file_path,
        epoch_time_ms_t reference_timestamp
) -> ErrorCode {
    static_assert(std::is_same_v<encoded_variable_t, four_byte_encoded_variable_t>);

    init_states();

    m_writer.open(file_path, FileWriter::OpenMode::CREATE_FOR_WRITING);
    m_zstd_compressor.open(m_writer);
    // For now, use preset metadata for the preamble
    if (false
        == clp::ffi::ir_stream::four_byte_encoding::serialize_preamble(
                TIMESTAMP_PATTERN,
                TIMESTAMP_PATTERN_SYNTAX,
                TIME_ZONE_ID,
                reference_timestamp,
                m_ir_buffer
        ))
    {
        SPDLOG_ERROR("Failed to serialize preamble");
        m_zstd_compressor.close();
        m_writer.close();
        return ErrorCode_Failure;
    }
    m_is_open = true;

    // Flush the preamble
    flush();

    return ErrorCode_Success;
}

template <typename encoded_variable_t>
auto LogEventSerializer<encoded_variable_t>::open(string const& file_path) -> ErrorCode {
    static_assert(std::is_same_v<encoded_variable_t, eight_byte_encoded_variable_t>);

    init_states();

    m_writer.open(file_path, FileWriter::OpenMode::CREATE_FOR_WRITING);
    m_zstd_compressor.open(m_writer);

    // For now, use preset metadata for the preamble
    if (false
        == clp::ffi::ir_stream::eight_byte_encoding::serialize_preamble(
                TIMESTAMP_PATTERN,
                TIMESTAMP_PATTERN_SYNTAX,
                TIME_ZONE_ID,
                m_ir_buffer
        ))
    {
        SPDLOG_ERROR("Failed to serialize preamble");
        m_zstd_compressor.close();
        m_writer.close();
        return ErrorCode_Failure;
    }
    m_is_open = true;

    // Flush the preamble
    flush();

    return ErrorCode_Success;
}

template <typename encoded_variable_t>
auto LogEventSerializer<encoded_variable_t>::close() -> void {
    m_ir_buffer.push_back(clp::ffi::ir_stream::cProtocol::Eof);
    flush();
    m_zstd_compressor.close();
    m_writer.close();
    m_is_open = false;
}

template <typename encoded_variable_t>
auto LogEventSerializer<encoded_variable_t>::flush() -> void {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCode_Unsupported, __FILENAME__, __LINE__);
    }
    m_zstd_compressor.write(reinterpret_cast<char const*>(m_ir_buffer.data()), m_ir_buffer.size());
    m_serialized_size += m_ir_buffer.size();
    m_ir_buffer.clear();
}

template <typename encoded_variable_t>
auto LogEventSerializer<encoded_variable_t>::serialize_log_event(
        string_view message,
        epoch_time_ms_t timestamp
) -> ErrorCode {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCode_Unsupported, __FILENAME__, __LINE__);
    }

    string logtype;
    bool res;
    if constexpr (std::is_same_v<encoded_variable_t, eight_byte_encoded_variable_t>) {
        res = clp::ffi::ir_stream::eight_byte_encoding::serialize_log_event(
                timestamp,
                message,
                logtype,
                m_ir_buffer
        );
    } else {
        auto timestamp_delta = timestamp - m_prev_msg_timestamp;
        m_prev_msg_timestamp = timestamp;
        res = clp::ffi::ir_stream::four_byte_encoding::serialize_log_event(
                timestamp_delta,
                message,
                logtype,
                m_ir_buffer
        );
    }
    if (false == res) {
        SPDLOG_ERROR("Failed to serialize log event: {}", message);
        return ErrorCode_Failure;
    }
    m_log_event_ix += 1;
    return ErrorCode_Success;
}

template <typename encoded_variable_t>
auto LogEventSerializer<encoded_variable_t>::init_states() -> void {
    m_serialized_size = 0;
    m_log_event_ix = 0;
    m_ir_buffer.clear();
}

// Explicitly declare template specializations so that we can define the template methods in this
// file
template LogEventSerializer<eight_byte_encoded_variable_t>::LogEventSerializer();
template LogEventSerializer<four_byte_encoded_variable_t>::LogEventSerializer();
template LogEventSerializer<eight_byte_encoded_variable_t>::~LogEventSerializer();
template LogEventSerializer<four_byte_encoded_variable_t>::~LogEventSerializer();
template auto LogEventSerializer<four_byte_encoded_variable_t>::open(
        string const& file_path,
        epoch_time_ms_t reference_timestamp
) -> ErrorCode;
template auto LogEventSerializer<eight_byte_encoded_variable_t>::open(string const& file_path
) -> ErrorCode;
template auto LogEventSerializer<four_byte_encoded_variable_t>::flush() -> void;
template auto LogEventSerializer<eight_byte_encoded_variable_t>::flush() -> void;
template auto LogEventSerializer<four_byte_encoded_variable_t>::close() -> void;
template auto LogEventSerializer<eight_byte_encoded_variable_t>::close() -> void;
template auto LogEventSerializer<eight_byte_encoded_variable_t>::serialize_log_event(
        string_view message,
        epoch_time_ms_t timestamp
) -> ErrorCode;
template auto LogEventSerializer<four_byte_encoded_variable_t>::serialize_log_event(
        string_view message,
        epoch_time_ms_t timestamp
) -> ErrorCode;

template auto LogEventSerializer<four_byte_encoded_variable_t>::init_states() -> void;
template auto LogEventSerializer<eight_byte_encoded_variable_t>::init_states() -> void;
}  // namespace clp::ir
