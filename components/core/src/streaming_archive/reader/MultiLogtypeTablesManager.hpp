#ifndef STREAMING_ARCHIVE_READER_GLT_MULITLOGTYPETABLE_MANAGER_HPP
#define STREAMING_ARCHIVE_READER_GLT_MULITLOGTYPETABLE_MANAGER_HPP

#include "LogtypeTableManager.hpp"
#include "CombinedLogtypeTable.hpp"

namespace streaming_archive::reader::glt {
    class MultiLogtypeTablesManager : public LogtypeTableManager {
    public:
        /**
         * Check if the 2D variable table is loaded for logtype_id
         * @param logtype_id
         * @return true if the variable column is loaded. Otherwise false
         */
        virtual void open(const std::string& segment_path) override;
        bool check_variable_column(logtype_dictionary_id_t logtype_id);
        void load_variable_columns(logtype_dictionary_id_t logtype_id);
        void get_variable_row_at_offset(logtype_dictionary_id_t logtype_id, size_t offset, GLTMessage& msg);
        epochtime_t get_timestamp_at_offset(logtype_dictionary_id_t logtype_id, size_t offset);
        void load_all_tables(combined_table_id_t combined_table_id);
        virtual void close() override;
    protected:
        // track of table which comes from a single compressed stream
        std::unordered_map<logtype_dictionary_id_t, LogtypeTable> m_logtype_tables;
        std::unordered_map<logtype_dictionary_id_t, CombinedLogtypeTable> m_combined_tables;
    };
}


#endif //STREAMING_ARCHIVE_READER_GLT_MULITLOGTYPETABLE_MANAGER_HPP
