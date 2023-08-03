#ifndef STREAMING_ARCHIVE_READER_GLT_MESSAGE_HPP
#define STREAMING_ARCHIVE_READER_GLT_MESSAGE_HPP

#include "../Message.hpp"

namespace streaming_archive::reader::glt {
    class GLTMessage : public Message {
    public:
        GLTMessage() : Message() {}
        file_id_t get_file_id () const;
        void set_file_id (file_id_t file_id);
        void resize_var (size_t var_size);
        std::vector<encoded_variable_t>& get_writable_vars ();
        void load_vars_from(const std::vector<encoded_variable_t>& vars, size_t count, size_t offset);
    private:
        file_id_t m_file_id;
    };
}

#endif //STREAMING_ARCHIVE_READER_GLT_MESSAGE_HPP
