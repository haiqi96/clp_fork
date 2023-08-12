#ifndef BUFFEREDFILEREADER_HPP
#define BUFFEREDFILEREADER_HPP

// C++ libraries
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// Project headers
#include "BufferReader.hpp"
#include "Defs.h"
#include "ErrorCode.hpp"
#include "ReaderInterface.hpp"
#include "TraceableException.hpp"

/**
 * Class for reading from a on-disk file with custom buffering.
 * The BufferedFileReader is designed to support files that only allow
 * sequential access, such as files in S3. The class uses a checkpoint
 * mechanism to support seeking and reading from a previous file position
 * without having to actually accessing the file.
 */
class BufferedFileReader : public ReaderInterface {
public:
    // Types
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}

        // Methods
        [[nodiscard]] auto what() const noexcept -> char const* override {
            return "BufferedFileReader operation failed";
        }
    };

    class OperationFailedWithMsg : public TraceableException {
    public:
        // Constructors
        OperationFailedWithMsg(
                ErrorCode error_code,
                char const* const filename,
                int line_number,
                std::string message
        )
                : TraceableException(error_code, filename, line_number),
                  m_message(std::move(message)) {}

        // Methods
        [[nodiscard]] auto what() const noexcept -> char const* override {
            return "BufferedFileReader operation failed";
        }

    private:
        std::string m_message;
    };

    // Constructors
    BufferedFileReader(size_t base_buffer_size);

    BufferedFileReader() : BufferedFileReader(cDefaultBufferSize) {}

    ~BufferedFileReader();

    // explicitly disable copy or move
    BufferedFileReader(BufferedFileReader const&) = delete;
    BufferedFileReader(BufferedFileReader&&) = delete;
    auto operator=(BufferedFileReader) -> BufferedFileReader& = delete;
    auto operator=(BufferedFileReader&&) -> BufferedFileReader& = delete;

    // Methods implementing the ReaderInterface
    /**
     * Tries to get the current position of the read head in the file
     * @param pos Position of the read head in the file
     * @return ErrorCode_NotInit if the file is not open
     * @return ErrorCode_errno on error
     * @return ErrorCode_Success on success
     */
    [[nodiscard]] auto try_get_pos(size_t& pos) -> ErrorCode override;
    /**
     * Tries to seek from the beginning of the file to the given position
     * @param pos
     * @return ErrorCode_NotInit if the file is not open
     * @return ErrorCode_errno on error
     * @return ErrorCode_Success on success
     */
    [[nodiscard]] auto try_seek_from_begin(size_t pos) -> ErrorCode override;

    /**
     * Tries to read up to a given number of bytes from the file
     * @param buf
     * @param num_bytes_to_read The number of bytes to try and read
     * @param num_bytes_read The actual number of bytes read
     * @return ErrorCode_NotInit if the file is not open
     * @return ErrorCode_BadParam if buf is invalid
     * @return ErrorCode_errno on error
     * @return ErrorCode_EndOfFile on EOF
     * @return ErrorCode_Success on success
     */
    [[nodiscard]] auto try_read(char* buf, size_t num_bytes_to_read, size_t& num_bytes_read)
            -> ErrorCode override;

    /**
     * Tries to read a string from the file until it reaches
     * the specified delimiter
     * @param delim The delimiter to stop at
     * @param keep_delimiter Whether to include the delimiter in the
     * output string or not
     * @param append Whether to append to the given string or
     * replace its contents
     * @param str The string read
     * @return ErrorCode_Success on success
     * @return ErrorCode_EndOfFile on EOF
     * @return ErrorCode_errno otherwise
     */
    [[nodiscard]] auto
    try_read_to_delimiter(char delim, bool keep_delimiter, bool append, std::string& str)
            -> ErrorCode override;

    // Methods
    [[nodiscard]] auto is_open() const -> bool { return -1 != m_fd; }

    /**
     * Tries to open a file
     * @param path
     * @return ErrorCode_Success on success
     * @return ErrorCode_FileNotFound if the file was not found
     * @return ErrorCode_errno otherwise
     */
    [[nodiscard]] auto try_open(std::string const& path) -> ErrorCode;
    /**
     * Opens a file
     * @param path
     * @throw BufferedFileReader::OperationFailed on failure
     */
    auto open(std::string const& path) -> void;
    /**
     * Closes the file if it's open
     */
    [[nodiscard]] auto close() -> ErrorCode;

    [[nodiscard]] auto get_path() const -> std::string const& { return m_path; }

    /**
     * Peeks the buffer without advancing the file
     * pos.
     * Note: If further operation such as read or peek is called on the
     * BufferedFileReader after peek_buffered_data, the buf could
     * point to invalid data
     * @param buf pointer pointing to peeked data
     * @param peek_size returns number of bytes peeked by reference
     * @return ErrorCode_Success on success
     * @return ErrorCode_errno on error
     * @return ErrorCode_NotInit if the file is not opened
     * @return ErrorCode_EndOfFile if already reaching the eof
     */
    [[nodiscard]] auto peek_buffered_data(char const*& data_ptr, size_t& peek_size) -> ErrorCode;

    /**
     * Sets a checkpoint at the current file pos.
     * By default, the checkpoint is not set and the BufferedFileReader only
     * maintains a fixed size buffer. Seeking before the reading pos is not
     * supported since the data might not be in the buffer anymore.
     *
     * When the checkpoint is set, the BufferedFileReader increases its
     * internal buffer size on demand and buffer all data between the
     * checkpoint pos and largest ever file_pos in the memory.
     * It then support seeking back to a previous file pos that's after the
     * checkpoint pos, as the data is guaranteed to be available in the internal
     * buffer.
     *
     * Note: Setting a checkpoint may result in higher memory usage since
     * the BufferedFileReader needs to exhaustively buffer the data it reads
     * in the buffer.
     * @return current file pos
     */
    auto set_checkpoint() -> size_t;

    /**
     * Disable the checkpoint pos and release buffered data from memory
     * The function resize the internal buffer based on the following rules.
     * 1. If the current reading_pos is within the same m_buffer_size region as
     * the buffer end pos (the file pos that end of buffer corresponds to). i.e.
     * buffer_end_pos - file_pos < m_buffer_size
     * the buffer will be resized to m_buffer_size bytes
     * 2. Else, The buffer will be resized to the rounded result of
     * quantizing (buffer_end_pos - file_pos) to the nearest multiple of
     * 'm_buffer_size' using the rounding method. This ensures that the current
     * read pos still resides in the resized buffer
     */
    auto clear_checkpoint() -> void;

private:
    // Methods
    /**
     * Quantize the given size to be the next integer multiple of buffer_size
     * @param size
     * @return quantized size
     */
    [[nodiscard]] auto quantize_to_buffer_size(size_t size) const -> size_t;

    /**
     * Reads next refill_size bytes from file descriptor to the internal buffer
     * and sets the data size of the internal buffer
     * Note: the function returns success even if the number of bytes read from
     * the fd is less than the refill_size
     * @param refill_size
     * @return ErrorCode_Success on success
     * @return ErrorCode_errno on error
     * @return ErrorCode_NotInit if the file is not opened
     * @return ErrorCode_EndOfFile if already reaching the eof
     */
    [[nodiscard]] auto refill_reader_buffer(size_t refill_size) -> ErrorCode;

    /**
     * Discard the data before pos from internal reader buffer and resize the
     * buffer
     * @param pos
     */
    auto resize_buffer_from_pos(size_t pos) -> void;

    /**
     * return the file_pos's corresponding pos in the internal buffer
     * @param file_pos
     * @return
     */
    [[nodiscard]] auto get_buffer_relative_pos(size_t file_pos) const -> size_t {
        return file_pos - m_buffer_begin_pos;
    }

    [[nodiscard]] auto get_buffer_end_pos() const -> size_t {
        return m_buffer_begin_pos + m_buffer_reader->get_buffer_size();
    }

    auto update_file_pos(size_t pos) -> void;

    // Constants
    static constexpr size_t cMinBufferSize = (1ULL << 12);
    static constexpr size_t cDefaultBufferSize = (16 * cMinBufferSize);

    // Variables
    int m_fd{-1};
    std::string m_path;
    size_t m_file_pos{0};

    // Buffer specific data
    std::vector<char> m_buffer;
    size_t m_base_buffer_size;
    std::optional<BufferReader> m_buffer_reader;
    size_t m_buffer_begin_pos{0};

    // Variables for checkpoint support
    std::optional<size_t> m_checkpoint_pos;
    size_t m_highest_read_pos{0};
};

#endif  // BUFFEREDFILEREADER_HPP