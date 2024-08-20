#include "SysFileReader.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include <cstddef>
#include <cstdio>
#include <span>

#include "ErrorCode.hpp"

using std::span;

namespace clp {
auto SysFileReader::try_read(char* buf, size_t num_bytes_to_read, size_t& num_bytes_read)
        -> ErrorCode {
    if (nullptr == buf) {
        return ErrorCode_BadParam;
    }

    num_bytes_read = 0;
    span dst_view{buf, num_bytes_to_read};
    while (true) {
        auto const bytes_read = ::read(m_fd.get_raw_fd(), dst_view.data(), num_bytes_to_read);
        if (0 == bytes_read) {
            break;
        }
        if (bytes_read < 0) {
            return ErrorCode_errno;
        }
        dst_view = dst_view.subspan(num_bytes_to_read);
        num_bytes_read += bytes_read;
        num_bytes_to_read -= bytes_read;
        if (0 == num_bytes_to_read) {
            return ErrorCode_Success;
        }
    }
    if (0 == num_bytes_read) {
        return ErrorCode_EndOfFile;
    }
    return ErrorCode_Success;
}

auto SysFileReader::try_seek_from_begin(size_t pos) -> ErrorCode {
    if (auto const offset = lseek(m_fd.get_raw_fd(), static_cast<off_t>(pos), SEEK_SET);
        static_cast<off_t>(-1) == offset)
    {
        return ErrorCode_errno;
    }

    return ErrorCode_Success;
}

auto SysFileReader::try_get_pos(size_t& pos) -> ErrorCode {
    auto const curr_offset = lseek(m_fd.get_raw_fd(), 0, SEEK_CUR);
    if (static_cast<off_t>(-1) == curr_offset) {
        return ErrorCode_errno;
    }
    pos = static_cast<size_t>(curr_offset);
    return ErrorCode_Success;
}
}  // namespace clp
