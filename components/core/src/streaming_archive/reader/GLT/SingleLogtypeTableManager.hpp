#ifndef CLP_SINGLELOGTYPETABLEMANAGER_HPP
#define CLP_SINGLELOGTYPETABLEMANAGER_HPP

// Project headers
#include "LogtypeTableManager.hpp"
#include "CombinedLogtypeTable.hpp"
#include "../../../Query.hpp"
#include <map>

namespace streaming_archive::reader::glt {

    class SingleLogtypeTableManager : public LogtypeTableManager {
    public:
        SingleLogtypeTableManager () :
                  m_single_table_loaded(false) {};
        void load_single_table (logtype_dictionary_id_t logtype_id);
        void close_single_table ();
        bool get_next_row (GLTMessage& msg);
        bool peek_next_ts(epochtime_t& ts);
        void load_all();
        void skip_row();
        void load_partial_columns(size_t l, size_t r);
        void load_ts();

        void rearrange_queries(const std::unordered_map<logtype_dictionary_id_t, LogtypeQueries>& src_queries,
                               std::vector<LogtypeQueries>& single_table_queries, std::map<combined_table_id_t, std::vector<LogtypeQueries>>& combined_table_queries);

        void open_combined_table(combined_table_id_t table_id);
        void open_and_preload_combined_table (combined_table_id_t table_id, logtype_dictionary_id_t logtype_id);
        void open_preloaded_combined_logtype_table (logtype_dictionary_id_t logtype_id);
        void close_combined_table();
        void open_combined_logtype_table (logtype_dictionary_id_t logtype_id);

        bool m_single_table_loaded;
        LogtypeTable m_single_table;
        CombinedLogtypeTable m_combined_table;

        // compressor for combined table. try to reuse only one compressor
#if USE_PASSTHROUGH_COMPRESSION
        streaming_compression::passthrough::Decompressor m_combined_table_decompressor;
#elif USE_ZSTD_COMPRESSION
        streaming_compression::zstd::Decompressor m_combined_table_decompressor;
#else
        static_assert(false, "Unsupported compression mode.");
#endif

    };
}


#endif //CLP_SINGLELOGTYPETABLEMANAGER_HPP