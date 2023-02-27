// C++ libraries
#include <unordered_set>

// Boost libraries
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

// spdlog
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

// Project headers
#include "../Profiler.hpp"
#include "../Utils.hpp"
#include "../clp/utils.hpp"
#include "Decoder.hpp"
#include "CommandLineArguments.hpp"

using ir_decoder::CommandLineArguments;
using ir_decoder::Decoder;
using std::string;
using std::unordered_set;
using std::vector;


bool decode (string ir_path, string output_path)
{
    Decoder ir_stream_decoder;
    ir_stream_decoder.decode(ir_path);
    return true;
}


int main (int argc, const char* argv[]) {
    // Program-wide initialization
    try {
        auto stderr_logger = spdlog::stderr_logger_st("stderr");
        spdlog::set_default_logger(stderr_logger);
        spdlog::set_pattern("%Y-%m-%d %H:%M:%S,%e [%l] %v");
    } catch (std::exception& e) {
        // NOTE: We can't log an exception if the logger couldn't be constructed
        return -1;
    }
    Profiler::init();
    TimestampPattern::init();

    CommandLineArguments command_line_args("ir_decoder");
    auto parsing_result = command_line_args.parse_arguments(argc, argv);
    switch (parsing_result) {
        case CommandLineArgumentsBase::ParsingResult::Failure:
            return -1;
        case CommandLineArgumentsBase::ParsingResult::InfoCommand:
            return 0;
        case CommandLineArgumentsBase::ParsingResult::Success:
            // Continue processing
            break;
    }

    string input_path = command_line_args.get_ir_path();
    if (boost::filesystem::exists(input_path) == false) {
        SPDLOG_ERROR("'{}' does not exist.", input_path.c_str());
        return -1;
    }

    string output_path = command_line_args.get_output_path();
    if (boost::filesystem::exists(output_path) == true) {
        SPDLOG_ERROR("'{}' already exists.", output_path.c_str());
        return -1;
    }

    bool decode_successful;
    try {
        decode_successful = decode(input_path, output_path);
    } catch (TraceableException& e) {
        ErrorCode error_code = e.get_error_code();
        if (ErrorCode_errno == error_code) {
            SPDLOG_ERROR("Compression failed: {}:{} {}, errno={}", e.get_filename(), e.get_line_number(), e.what(), errno);
            decode_successful = false;
        } else {
            SPDLOG_ERROR("Compression failed: {}:{} {}, error_code={}", e.get_filename(), e.get_line_number(), e.what(), error_code);
            decode_successful = false;
        }
    } catch (std::exception& e) {
        SPDLOG_ERROR("Compression failed: Unexpected exception - {}", e.what());
        decode_successful = false;
    }
    if (!decode_successful) {
        return -1;
    }
    return 0;
}