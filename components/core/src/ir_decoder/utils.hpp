//
// Created by Haiqi on 2023/2/26.
//

#ifndef IR_DECODER_UTILS_HPP
#define IR_DECODER_UTILS_HPP

// C++ standard libraries
#include <string>

namespace ir_decoder {
    bool is_clp_magic_number(size_t sequence_length, const char* sequence, bool& is_compacted);
}


#endif //IR_DECODER_UTILS_HPP
