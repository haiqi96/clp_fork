//
// Created by Haiqi on 2023/3/10.
//

#ifndef STREAMING_ARCHIVE_READER_IRMESSAGE_HPP
#define STREAMING_ARCHIVE_READER_IRMESSAGE_HPP

#include "../../TraceableException.hpp"
#include "../../ffi/encoding_methods.hpp"
#include <string>
#include "../../Defs.h"
#include <vector>

namespace streaming_archive::reader {
    class IRMessage {
    public:
        // Types
        class OperationFailed : public TraceableException {
        public:
            // Constructors
            OperationFailed (ErrorCode error_code, const char* const filename, int line_number) : TraceableException (error_code, filename, line_number) {}

            // Methods
            const char* what() const noexcept override {
                return "EncodedParsedMessage operation failed";
            }
        };
        IRMessage() {}
        void clear();
        void append_int_vars(encoded_variable_t var);
        void append_float_vars(encoded_variable_t var);
        void append_dict_vars(std::string dictionary_var);
        void set_time(epochtime_t t) { m_ts = t; }
        void logtype_append(const std::string& logtype_str, size_t begin, size_t length);

        const std::vector<std::string>& get_dictionary_vars() const {
            return m_dictionary_vars;
        };
        const std::vector<encoded_variable_t>& get_encoded_vars() const {
            return m_encoded_vars;
        };
        const std::string& get_log_type() const {
            return m_log_type;
        };
        const std::vector<bool>& get_var_types() const {
            return m_is_dict;
        }
        epochtime_t get_timestamp() const {
            return m_ts;
        }

    private:
        std::vector<std::string> m_dictionary_vars;
        std::vector<encoded_variable_t> m_encoded_vars;
        std::string m_log_type;
        // true is encoded, not true is string vars
        std::vector<bool> m_is_dict;
        epochtime_t m_ts;
    };
}

#endif //STREAMING_ARCHIVE_READER_IRMESSAGE_HPP
