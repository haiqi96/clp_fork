#include "run.hpp"

#include <unordered_set>

#include <log_surgeon/LogParser.hpp>
#include <spdlog/sinks/stdout_sinks.h>

#include "../Profiler.hpp"
#include "../spdlog_with_specializations.hpp"
#include "../Utils.hpp"
#include "CommandLineArguments.hpp"
#include "compression.hpp"
#include "decompression.hpp"
#include "utils.hpp"
#include "../aws/AwsAuthenticationSigner.hpp"

using std::string;
using std::unordered_set;
using std::vector;
using clp::aws::AwsAuthenticationSigner;
using clp::aws::S3Url;

namespace clp::clp {
int run(int argc, char const* argv[]) {
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

    CommandLineArguments command_line_args("clp");
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

    vector<string> input_paths = command_line_args.get_input_paths();

    Profiler::start_continuous_measurement<Profiler::ContinuousMeasurementIndex::Compression>();

    // Read input paths from file if necessary
    if (false == command_line_args.get_path_list_path().empty()) {
        if (false == read_input_paths(command_line_args.get_path_list_path(), input_paths)) {
            return -1;
        }
    }

    auto command = command_line_args.get_command();
    if (CommandLineArguments::Command::Compress == command) {
        /// TODO: make this not a unique_ptr and test performance difference
        std::unique_ptr<log_surgeon::ReaderParser> reader_parser;
        if (!command_line_args.get_use_heuristic()) {
            std::string const& schema_file_path = command_line_args.get_schema_file_path();
            reader_parser = std::make_unique<log_surgeon::ReaderParser>(schema_file_path);
        }

        // Get paths of all files we need to compress
        vector<FileToCompress> files_to_compress;
        vector<FileToCompress> grouped_files_to_compress;
        vector<string> empty_directory_paths;
        if (CommandLineArguments::InputSource::S3 == command_line_args.get_input_source()) {
            string const access_key_id{getenv("AWS_ACCESS_KEY_ID")};
            string const secret_access_key{getenv("AWS_SECRET_ACCESS_KEY")};
            if (access_key_id.empty()) {
                SPDLOG_ERROR("AWS_ACCESS_KEY_ID environment variable is not set");
                return -1;
            }
            if (secret_access_key.empty()) {
                SPDLOG_ERROR("AWS_SECRET_ACCESS_KEY environment variable is not set");
                return -1;
            }
            AwsAuthenticationSigner aws_auth_signer{access_key_id, secret_access_key};
            for (auto const& input_path : input_paths) {
                try {
                    S3Url const s3_url{input_path};
                    string presigned_url{};
                    if (auto error_code = aws_auth_signer.generate_presigned_url(s3_url, presigned_url);
                        ErrorCode_Success != error_code) {
                        SPDLOG_ERROR("Failed to generate s3 presigned url, error: {}", error_code);
                        return -1;
                    }
                    files_to_compress.emplace_back(presigned_url, s3_url.get_compression_path(), 0);
                    std::cout << presigned_url << std::endl;
                    std::cout << s3_url.get_compression_path() << std::endl;
                } catch (S3Url::OperationFailed const& err) {
                    SPDLOG_ERROR(err.what());
                    return -1;
                }
            }
        } else if ((CommandLineArguments::InputSource::Filesystem
                    == command_line_args.get_input_source()))
        {
            boost::filesystem::path path_prefix_to_remove(
                    command_line_args.get_path_prefix_to_remove()
            );

            // Validate input paths exist
            if (false == validate_paths_exist(input_paths)) {
                return -1;
            }

            for (auto const& input_path : input_paths) {
                if (false
                    == find_all_files_and_empty_directories(
                            path_prefix_to_remove,
                            input_path,
                            files_to_compress,
                            empty_directory_paths
                    ))
                {
                    return -1;
                }
            }

            if (files_to_compress.empty() && empty_directory_paths.empty()
                && grouped_files_to_compress.empty())
            {
                SPDLOG_ERROR("No files/directories to compress.");
                return -1;
            }
        }

        bool compression_successful;
        try {
            compression_successful = compress(
                    command_line_args,
                    files_to_compress,
                    empty_directory_paths,
                    grouped_files_to_compress,
                    command_line_args.get_target_encoded_file_size(),
                    std::move(reader_parser),
                    command_line_args.get_use_heuristic()
            );
        } catch (TraceableException& e) {
            ErrorCode error_code = e.get_error_code();
            if (ErrorCode_errno == error_code) {
                SPDLOG_ERROR(
                        "Compression failed: {}:{} {}, errno={}",
                        e.get_filename(),
                        e.get_line_number(),
                        e.what(),
                        errno
                );
                compression_successful = false;
            } else {
                SPDLOG_ERROR(
                        "Compression failed: {}:{} {}, error_code={}",
                        e.get_filename(),
                        e.get_line_number(),
                        e.what(),
                        error_code
                );
                compression_successful = false;
            }
        } catch (std::exception& e) {
            SPDLOG_ERROR("Compression failed: Unexpected exception - {}", e.what());
            compression_successful = false;
        }
        if (!compression_successful) {
            return -1;
        }
    } else if (CommandLineArguments::Command::Extract == command) {
        unordered_set<string> files_to_decompress(input_paths.cbegin(), input_paths.cend());
        if (false == decompress(command_line_args, files_to_decompress)) {
            return -1;
        }
    } else if (CommandLineArguments::Command::ExtractIr == command) {
        if (false == decompress_to_ir(command_line_args)) {
            return -1;
        }
    } else {
        SPDLOG_ERROR("Command {} not implemented.", enum_to_underlying_type(command));
        return -1;
    }

    Profiler::stop_continuous_measurement<Profiler::ContinuousMeasurementIndex::Compression>();
    LOG_CONTINUOUS_MEASUREMENT(Profiler::ContinuousMeasurementIndex::Compression)

    return 0;
}
}  // namespace clp::clp
