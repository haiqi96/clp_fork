#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <Catch2/single_include/catch2/catch.hpp>

#include "../src/clp/ffi/ir_stream/decoding_methods.hpp"
#include "../src/clp/ir/constants.hpp"
#include "../src/clp/ir/LogEventDeserializer.hpp"
#include "../src/clp/ir/LogEventSerializer.hpp"
#include "../src/clp/ir/types.hpp"
#include "../src/clp/streaming_compression/zstd/Decompressor.hpp"

using clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
using clp::ir::cIrFileExtension;
using clp::ir::eight_byte_encoded_variable_t;
using clp::ir::epoch_time_ms_t;
using clp::ir::four_byte_encoded_variable_t;
using clp::ir::LogEventDeserializer;
using clp::ir::LogEventSerializer;
using clp::streaming_compression::zstd::Decompressor;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::is_same_v;
using std::string;
using std::vector;

namespace {
struct TestLogEvent {
    epoch_time_ms_t timestamp;
    string msg;
};
}  // namespace

/*
 * The test case only covers four byte encoding because decompressor
 * does not support eight bytes encoding yet.
 */
TEMPLATE_TEST_CASE(
        "Encode and serialize log events",
        "[ir][serialize-log-event]",
        four_byte_encoded_variable_t,
        eight_byte_encoded_variable_t
) {
    LogEventSerializer<TestType> serializer;

    // Test encoding with serializer
    vector<string> var_strs
            = {"4938",
               std::to_string(INT32_MAX),
               std::to_string(INT64_MAX),
               "0.1",
               "-25.519686",
               "-25.5196868642755",
               "-00.00",
               "bin/python2.7.3",
               "abc123"};
    size_t var_ix{0};

    vector<TestLogEvent> test_log_events;

    auto const ts_1 = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    string log_event_1 = "Here is the first string with a small int " + var_strs[var_ix++];
    log_event_1 += " and a medium int " + var_strs[var_ix++];
    log_event_1 += " and a very large int " + var_strs[var_ix++];
    log_event_1 += " and a small float " + var_strs[var_ix++];
    log_event_1 += "\n";
    test_log_events.push_back({ts_1, log_event_1});

    auto const ts_2 = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    auto log_event_2 = "Here is the second string with a medium float " + var_strs[var_ix++];
    log_event_2 += " and a weird float " + var_strs[var_ix++];
    log_event_2 += " and a string with numbers " + var_strs[var_ix++];
    log_event_2 += " and another string with numbers " + var_strs[var_ix++];
    log_event_2 += "\n";
    test_log_events.push_back({ts_2, log_event_2});

    string ir_test_file = "ir_serializer_test";
    ir_test_file += cIrFileExtension;

    REQUIRE(serializer.open(ir_test_file));
    // Test serializing log events
    for (auto const& test_log_event : test_log_events) {
        REQUIRE(serializer.serialize_log_event(test_log_event.timestamp, test_log_event.msg));
    }
    serializer.close();

    Decompressor ir_reader;
    ir_reader.open(ir_test_file);

    bool uses_four_byte_encoding{false};
    REQUIRE(
            (IRErrorCode_Success
             == clp::ffi::ir_stream::get_encoding_type(ir_reader, uses_four_byte_encoding))
    );
    REQUIRE((is_same_v<TestType, four_byte_encoded_variable_t> == uses_four_byte_encoding));

    auto result = LogEventDeserializer<TestType>::create(ir_reader);
    REQUIRE((false == result.has_error()));
    auto& deserializer = result.value();

    // Decode and deserialize all expected log events
    for (auto const& test_log_event : test_log_events) {
        auto deserialized_result = deserializer.deserialize_log_event();
        REQUIRE((false == deserialized_result.has_error()));

        auto& log_event = deserialized_result.value();
        auto const decoded_message = log_event.get_message().decode_and_unparse();
        REQUIRE(decoded_message.has_value());

        REQUIRE((decoded_message.value() == test_log_event.msg));
        REQUIRE((log_event.get_timestamp() == test_log_event.timestamp));
    }
    // Try decoding a nonexistent log event
    auto deserialized_result = deserializer.deserialize_log_event();
    REQUIRE(deserialized_result.has_error());

    std::filesystem::remove(ir_test_file);
}
