#ifndef STREAMING_ARCHIVE_LOGTYPESIZETRACKER_HPP
#define STREAMING_ARCHIVE_LOGTYPESIZETRACKER_HPP

#include "../Defs.h"
#include <cstdio>
#include "Constants.hpp"

namespace streaming_archive::glt {
    class LogtypeSizeTracker {
    /**
     * Class representing the size of a logtype table in GLT.
     * When two table has the same size, they are ordered base on logtype ID
     */
    public:

        size_t get_size() const {
            return m_size;
        }
        logtype_dictionary_id_t get_id() const {
            return m_logtype_id;
        }

        static size_t get_table_size(size_t num_columns, size_t num_rows) {
            size_t var_size = num_rows * num_columns * sizeof(encoded_variable_t);
            size_t ts_size = num_rows * sizeof(epochtime_t);
            size_t file_id_size = num_rows * sizeof(file_id_t);
            return var_size + ts_size + file_id_size;
        }

        bool operator< (const LogtypeSizeTracker& val) const {
            if (m_size == val.m_size) {
                return m_logtype_id < val.m_logtype_id;
            }
            return m_size < val.m_size;
        }

        bool operator> (const LogtypeSizeTracker& val) const {
            if (m_size == val.m_size) {
                return m_logtype_id > val.m_logtype_id;
            }
            return m_size > val.m_size;
        }

        LogtypeSizeTracker (logtype_dictionary_id_t logtype_id, size_t logtype_size) {
            this->m_size = logtype_size;
            this->m_logtype_id = logtype_id;
        }

        LogtypeSizeTracker (logtype_dictionary_id_t logtype_id, size_t num_columns,
                            size_t num_rows) {
            // size of variables
            size_t logtype_size = num_rows * num_columns * sizeof(encoded_variable_t);
            // size of timestamp and file-id
            logtype_size += num_rows * (sizeof(epochtime_t) + sizeof(file_id_t));
            this->m_size = logtype_size;
            this->m_logtype_id = logtype_id;
        }
    private:
        size_t m_size;
        logtype_dictionary_id_t m_logtype_id;
    };
}
#endif //STREAMING_ARCHIVE_LOGTYPESIZETRACKER_HPP
