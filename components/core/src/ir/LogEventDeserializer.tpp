#ifndef IR_LOGEVENTDESERIALIZER_TPP
#define IR_LOGEVENTDESERIALIZER_TPP

#include <vector>

#include <json/single_include/nlohmann/json.hpp>

#include "../ffi/ir_stream/decoding_methods.hpp"
#include "LogEventDeserializer.hpp"

namespace ir {
template <typename encoded_variable_t>
auto LogEventDeserializer<encoded_variable_t>::create(ReaderInterface& reader)
        -> BOOST_OUTCOME_V2_NAMESPACE::std_result<LogEventDeserializer<encoded_variable_t>> {
    ffi::ir_stream::encoded_tag_t metadata_type{0};
    std::vector<int8_t> metadata;
    auto ir_error_code = ffi::ir_stream::decode_preamble(reader, metadata_type, metadata);
    if (ffi::ir_stream::IRErrorCode_Success != ir_error_code) {
        switch (ir_error_code) {
            case ffi::ir_stream::IRErrorCode_Incomplete_IR:
                return std::errc::result_out_of_range;
            case ffi::ir_stream::IRErrorCode_Corrupted_IR:
            default:
                return std::errc::protocol_error;
        }
    }

    if (ffi::ir_stream::cProtocol::Metadata::EncodingJson != metadata_type) {
        return std::errc::protocol_not_supported;
    }

    // Parse metadata and validate version
    auto metadata_json = nlohmann::json::parse(metadata, nullptr, false);
    if (metadata_json.is_discarded()) {
        return std::errc::protocol_error;
    }
    auto version_iter = metadata_json.find(ffi::ir_stream::cProtocol::Metadata::VersionKey);
    if (metadata_json.end() == version_iter || false == version_iter->is_string()) {
        return std::errc::protocol_error;
    }
    auto metadata_version = version_iter->get_ref<nlohmann::json::string_t&>();
    if (static_cast<char const*>(ffi::ir_stream::cProtocol::Metadata::VersionValue)
        != metadata_version)
    {
        return std::errc::protocol_not_supported;
    }

    if constexpr (std::is_same_v<encoded_variable_t, ffi::eight_byte_encoded_variable_t>) {
        return LogEventDeserializer<encoded_variable_t>{reader};
    } else if constexpr (std::is_same_v<encoded_variable_t, ffi::four_byte_encoded_variable_t>) {
        // Get reference timestamp
        auto ref_timestamp_iter
                = metadata_json.find(ffi::ir_stream::cProtocol::Metadata::ReferenceTimestampKey);
        if (metadata_json.end() == ref_timestamp_iter || false == ref_timestamp_iter->is_string()) {
            return std::errc::protocol_error;
        }
        auto ref_timestamp_str = ref_timestamp_iter->get_ref<nlohmann::json::string_t&>();
        ffi::epoch_time_ms_t ref_timestamp{};
        if (false == convert_string_to_int(ref_timestamp_str, ref_timestamp)) {
            return std::errc::protocol_error;
        }

        return LogEventDeserializer<encoded_variable_t>{reader, ref_timestamp};
    } else {
        static_assert(cAlwaysFalse<encoded_variable_t>);
    }
}

template <typename encoded_variable_t>
auto LogEventDeserializer<encoded_variable_t>::deserialize_log_event()
        -> BOOST_OUTCOME_V2_NAMESPACE::std_result<LogEvent<encoded_variable_t>> {
    ffi::epoch_time_ms_t timestamp_or_timestamp_delta{};
    std::string logtype;
    std::vector<std::string> dict_vars;
    std::vector<encoded_variable_t> encoded_vars;

    auto ir_error_code = ffi::ir_stream::deserialize_ir_message(
            m_reader,
            logtype,
            encoded_vars,
            dict_vars,
            timestamp_or_timestamp_delta
    );
    if (ffi::ir_stream::IRErrorCode_Success != ir_error_code) {
        switch (ir_error_code) {
            case ffi::ir_stream::IRErrorCode_Eof:
                return std::errc::no_message_available;
            case ffi::ir_stream::IRErrorCode_Incomplete_IR:
                return std::errc::result_out_of_range;
            case ffi::ir_stream::IRErrorCode_Corrupted_IR:
            default:
                return std::errc::protocol_error;
        }
    }

    ffi::epoch_time_ms_t timestamp{};
    if constexpr (std::is_same_v<encoded_variable_t, ffi::eight_byte_encoded_variable_t>) {
        timestamp = timestamp_or_timestamp_delta;
    } else if constexpr (std::is_same_v<encoded_variable_t, ffi::four_byte_encoded_variable_t>) {
        m_prev_msg_timestamp += timestamp_or_timestamp_delta;
        timestamp = m_prev_msg_timestamp;
    } else {
        static_assert(cAlwaysFalse<encoded_variable_t>);
    }

    return LogEvent<encoded_variable_t>{timestamp, logtype, dict_vars, encoded_vars};
}
}  // namespace ir

#endif  // IR_LOGEVENTDESERIALIZER_TPP
