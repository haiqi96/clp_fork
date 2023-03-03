#ifndef CLP_ENCODEDPARSEDMESSAGE_HPP
#define CLP_ENCODEDPARSEDMESSAGE_HPP

#include "TimestampPattern.hpp"
#include "Defs.h"
#include <vector>

class ParsedIRMessage {
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
    ParsedIRMessage() {
        m_ts_patt = nullptr;
    }

    // TODO: Looks like a memory leak to me. leave it here
    void clear();
    void clear_except_ts_patt();
    void set_ts_pattern(uint8_t num_spaces_before_ts, const std::string& format);
    void append_encoded_vars(encoded_variable_t var) { m_encoded_vars.push_back(var);};
    void set_log_type(std::string& log_type) { m_log_type = log_type; }
    void append_dict_vars(std::string dictionary_var) { m_dictionary_vars.push_back(dictionary_var); }
    void set_time(epochtime_t t) { m_ts = t; }
    void add_placeholder(size_t pos) { m_placeholder_pos.push_back(pos); }
    void set_compact(bool is_compact) { m_is_compact = is_compact; }
    bool is_compact() const { return m_is_compact; }

    void recover_message(std::string& message);

    void recover_message_deprecated(std::string& message);

    const std::vector<size_t>& get_placeholder_pos() const {
        return m_placeholder_pos;
    }

    const std::vector<std::string>& get_dictionary_vars() const {
        return m_dictionary_vars;
    };
    const std::vector<encoded_variable_t>& get_encoded_vars() const {
        return m_encoded_vars;
    };
    const std::string& get_log_type() const {
        return m_log_type;
    };
    epochtime_t get_timestamp() const {
        return m_ts;
    }
    const TimestampPattern* get_ts_patt() const {
        return m_ts_patt.get();
    }

    //TODO: update this
    const size_t get_original_bytes() const {
        return 16;
    }
private:
    // note, the scope of this m_ts_patt is for a whole file. which means
    // for the same parse_and_encode, the timestamp will last there forever
    std::unique_ptr<TimestampPattern> m_ts_patt;
    std::vector<std::string> m_dictionary_vars;
    std::vector<encoded_variable_t> m_encoded_vars;
    std::string m_log_type;
    // true is encoded, not true is string vars
    std::vector<size_t> m_placeholder_pos;
    epochtime_t m_ts;
    bool m_is_compact;
};


#endif //CLP_ENCODEDPARSEDMESSAGE_HPP
