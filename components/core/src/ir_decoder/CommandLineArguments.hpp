#ifndef IR_DECODER_COMMANDLINEARGUMENTS_HPP
#define IR_DECODER_COMMANDLINEARGUMENTS_HPP

// C++ libraries
#include <vector>
#include <string>

// Boost libraries
#include <boost/asio.hpp>

// Project headers
#include "../CommandLineArgumentsBase.hpp"
#include "../GlobalMetadataDBConfig.hpp"


namespace ir_decoder {
    class CommandLineArguments : public CommandLineArgumentsBase {
    public:
        // Constructors
        explicit CommandLineArguments (const std::string& program_name) : CommandLineArgumentsBase(program_name) {}

        // Methods
        ParsingResult parse_arguments (int argc, const char* argv[]) override;

        const std::string& get_ir_path () const { return m_ir_path; }
        const std::string& get_output_path () const { return m_output_path; }

    private:
        // Methods
        void print_basic_usage () const override;

        // Variables
        std::string m_ir_path;
        std::string m_output_path;
    };
}

#endif //IR_DECODER_COMMANDLINEARGUMENTS_HPP
