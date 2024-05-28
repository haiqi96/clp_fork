#ifndef CLP_IR_LOGEVENTDESERIALIZER_HPP
#define CLP_IR_LOGEVENTDESERIALIZER_HPP

#include <optional>

#include <boost-outcome/include/boost/outcome/std_result.hpp>

#include "../WriterInterface.hpp"
#include "../TimestampPattern.hpp"
#include "../TraceableException.hpp"
#include "../type_utils.hpp"
#include "LogEvent.hpp"
#include "types.hpp"

using std::string_view;

namespace clp::ir {
/**
 * Class for deserializing IR log events from an IR stream.
 *
 * TODO: We're currently returning std::errc error codes, but we should replace these with our own
 * custom error codes (derived from std::error_code), ideally replacing IRErrorCode.
 * @tparam encoded_variable_t Type of encoded variables in the stream
 */
template <typename encoded_variable_t>
class LogEventSerializer {
public:
    // Factory functions
    /**
     * Creates a log event Serializer for the given stream
     * @param reader A reader for the IR stream
     * @return A result containing the serializer or an error code indicating the failure:
     * - std::errc::result_out_of_range if the IR stream is truncated
     * - std::errc::protocol_error if the IR stream is corrupted
     * - std::errc::protocol_not_supported if the IR stream contains an unsupported metadata format
     *   or uses an unsupported version
     */
    static auto create(
            WriterInterface& writer,
            epoch_time_ms_t reference_timestamp
    ) -> BOOST_OUTCOME_V2_NAMESPACE::std_result<std::unique_ptr<LogEventSerializer<encoded_variable_t>>>;

    /**
     * Creates a log event Serializer for the given stream
     * @param reader A reader for the IR stream
     * @return A result containing the serializer or an error code indicating the failure:
     * - std::errc::result_out_of_range if the IR stream is truncated
     * - std::errc::protocol_error if the IR stream is corrupted
     * - std::errc::protocol_not_supported if the IR stream contains an unsupported metadata format
     *   or uses an unsupported version
     */
    static auto create(
            WriterInterface& writer
    ) -> BOOST_OUTCOME_V2_NAMESPACE::std_result<std::unique_ptr<LogEventSerializer<encoded_variable_t>>>;

    // Delete copy constructor and assignment
    LogEventSerializer(LogEventSerializer const&) = delete;
    auto operator=(LogEventSerializer const&) -> LogEventSerializer& = delete;

    // Define default move constructor and assignment
    LogEventSerializer(LogEventSerializer&&) = default;
    auto operator=(LogEventSerializer&&) -> LogEventSerializer& = default;

    ~LogEventSerializer() = default;

    [[nodiscard]] auto get_serialized_size() const -> size_t { return m_ir_buffer.size(); }
    [[nodiscard]] auto get_log_event_ix() const -> size_t { return m_log_event_ix; }

    auto flush() -> void;

    /**
     * Serializes a log event from the stream
     * @return A result containing the log event or an error code indicating the failure:
     * - std::errc::no_message_available on reaching the end of the IR stream
     * - std::errc::result_out_of_range if the IR stream is truncated
     * - std::errc::result_out_of_range if the IR stream is corrupted
     */
    [[nodiscard]] auto serialize_preamble(
        string_view timestamp_pattern,
        string_view timestamp_pattern_syntax,
        string_view time_zone_id
    ) -> bool;

    [[nodiscard]] auto serialize_preamble(
        string_view timestamp_pattern,
        string_view timestamp_pattern_syntax,
        string_view time_zone_id,
        epoch_time_ms_t reference_timestamp
    ) -> bool;

    /**
     * Serializes a log event from the stream
     * @return A result containing the log event or an error code indicating the failure:
     * - std::errc::no_message_available on reaching the end of the IR stream
     * - std::errc::result_out_of_range if the IR stream is truncated
     * - std::errc::result_out_of_range if the IR stream is corrupted
     */
    [[nodiscard]] auto serialize_log_event(
            string_view message,
            epoch_time_ms_t timestamp
    ) -> bool;

private:
    // Constructors
    explicit LogEventSerializer(WriterInterface& writer)
            : m_writer{writer},
              m_log_event_ix{0} {}

    LogEventSerializer(WriterInterface& writer, epoch_time_ms_t ref_timestamp)
            : m_writer{writer},
              m_prev_msg_timestamp{ref_timestamp},
              m_log_event_ix{0} {}

    // Variables
    size_t m_log_event_ix;
    [[no_unique_address]] std::conditional_t<
    std::is_same_v<encoded_variable_t, four_byte_encoded_variable_t>,
    epoch_time_ms_t,
    EmptyType> m_prev_msg_timestamp{};
    std::vector<int8_t> m_ir_buffer;
    WriterInterface& m_writer;
};
}  // namespace clp::ir

#endif //CLP_IR_LOGEVENTDESERIALIZER_HPP