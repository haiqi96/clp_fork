#ifndef CLP_ENCODEDPARSEDMESSAGE_HPP
#define CLP_ENCODEDPARSEDMESSAGE_HPP

#include "TimestampPattern.hpp"
#include "Defs.h"
#include <vector>

class EncodedParsedMessage {
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
    EncodedParsedMessage() {
        m_ts_patt = nullptr;
    }

    // TODO: Looks like a memory leak to me. leave it here
    void clear();
    void clear_except_ts_patt();
    TimestampPattern * get_ts_patt() {return m_ts_patt;};
    void set_timestamp(uint8_t num_spaces_before_ts, const std::string& format);
    void set_timezone(const std::string& timezone) {m_timezone = timezone;};
    void set_version(const std::string& version) {m_version = version;};
    void set_reference_timestamp(const epochtime_t input) { m_reference_timestamp = input; m_last_timestamp = input; }
    void set_encoding_version(const bool value) { is_compact_encoding = value;}
    void append_encoded_vars(encoded_variable_t var) { m_encoded_vars.push_back(var);};
    void set_log_msg(std::string& log_msg) {m_log_strings = log_msg;};
    void append_unencoded_vars(std::string unencoded_var) {m_unencoded_vars.push_back(unencoded_var);};
    void set_time(epochtime_t t) {m_ts = t;};
    void append_order(bool val) {m_order.push_back(val);};

    const std::vector<bool>& get_order() const {
        return m_order;
    };

    epochtime_t get_reference_timestamp() const { return m_reference_timestamp; }
    epochtime_t get_last_timestamp() const { return m_last_timestamp; }
    void set_last_timestamp(epochtime_t time_value) { m_last_timestamp = time_value; }

    std::string recover_message();

    const std::vector<std::string>& get_unencoded_vars() const {
        return m_unencoded_vars;
    };
    const std::vector<encoded_variable_t>& get_encoded_vars() const {
        return m_encoded_vars;
    };
    const std::string& get_log_msg() const {
        return m_log_strings;
    };
    epochtime_t get_timestamp() const {
        return m_ts;
    }


private:
    // note, the scope of this m_ts_patt is for a whole file. which means
    // for the same parse_and_encode, the timestamp will last there forever
    TimestampPattern * m_ts_patt;
    std::string m_timezone;
    epochtime_t m_reference_timestamp;
    epochtime_t m_last_timestamp;
    std::string m_version;
    std::vector<std::string> m_unencoded_vars;
    std::vector<encoded_variable_t> m_encoded_vars;
    std::string m_log_strings;
    bool is_compact_encoding;
    // true is encoded, not true is string vars
    std::vector<bool> m_order;
    epochtime_t m_ts;
};


#endif //CLP_ENCODEDPARSEDMESSAGE_HPP
