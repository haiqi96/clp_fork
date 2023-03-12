#include "IRMessage.hpp"

namespace streaming_archive::reader {
    void IRMessage::clear () {
        m_dictionary_vars.clear();
        m_encoded_vars.clear();
        m_log_type.clear();
        m_is_dict.clear();
    }

    void IRMessage::append_dict_vars (std::string dictionary_var) {
        m_dictionary_vars.push_back(dictionary_var);
        m_log_type += (char)ffi::VariablePlaceholder::Dictionary;
        m_is_dict.push_back(true);
    }

    void IRMessage::append_int_vars(encoded_variable_t var) {
        m_encoded_vars.push_back(var);
        m_log_type += (char)ffi::VariablePlaceholder::Integer;
        m_is_dict.push_back(false);
    }

    void IRMessage::append_float_vars(encoded_variable_t var) {
        m_encoded_vars.push_back(var);
        m_log_type += (char)ffi::VariablePlaceholder::Float;
        m_is_dict.push_back(false);
    }

    void IRMessage::logtype_append (const std::string& logtype_str, size_t begin, size_t length) {
        m_log_type.append(logtype_str, begin, length);
    }

}