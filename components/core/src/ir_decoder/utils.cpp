#include "utils.hpp"

#include <spdlog/spdlog.h>

namespace ir_decoder {
    bool is_clp_magic_number(size_t sequence_length, const char* sequence, bool& is_compacted) {
        constexpr int COMPACT_ENCODING = 0xfd2fb529;
        constexpr int STANDARD_ENCODING = 0xfd2fb530;

        if(sequence_length != 4) {
            SPDLOG_ERROR("Unexpected length");
            return false;
        }

        unsigned char byte1 = sequence[0];
        unsigned char byte2 = sequence[1];
        unsigned char byte3 = sequence[2];
        unsigned char byte4 = sequence[3];

        if(byte1 != 0xfd || byte2 != 0x2f || byte3 != 0xb5) {
            return false;
        }
        if(byte4 == 0x29) {
            is_compacted = true;
            return true;
        } else if (byte4 == 0x30) {
            is_compacted = false;
            return true;
        }
        return false;
    }
}