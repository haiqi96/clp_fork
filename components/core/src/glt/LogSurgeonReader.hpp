#ifndef GLT_LOG_SURGEON_READER_HPP
#define GLT_LOG_SURGEON_READER_HPP

#include <log_surgeon/Reader.hpp>

#include "ReaderInterface.hpp"

namespace glt {
/*
 * Wrapper providing a read function that works with the parsers in log_surgeon.
 */
class LogSurgeonReader : public log_surgeon::Reader {
public:
    LogSurgeonReader(ReaderInterface& reader_interface);

private:
    ReaderInterface& m_reader_interface;
};
}  // namespace glt

#endif  // GLT_LOG_SURGEON_READER_HPP
