#include "GLTMessage.hpp"

namespace streaming_archive::reader::glt {
    file_id_t GLTMessage::get_file_id () const {
        return m_file_id;
    }

    void GLTMessage::set_file_id (file_id_t file_id) {
        m_file_id = file_id;
    }

    std::vector<encoded_variable_t>& GLTMessage::get_writable_vars () {
        return m_vars;
    }

    void GLTMessage::resize_var (size_t var_size) {
        m_vars.resize(var_size);
    }

    void GLTMessage::load_vars_from (const std::vector<encoded_variable_t>& vars, size_t count, size_t offset) {
        for(size_t var_ix = 0; var_ix < count; var_ix++) {
            m_vars.at(var_ix) = vars.at(var_ix + offset);
        }
    }
}