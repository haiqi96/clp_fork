#include <array>

#include <filesystem>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <Catch2/single_include/catch2/catch.hpp>

#include "../src/clp/Array.hpp"
#include "../src/clp/BufferedFileReader.hpp"
#include "../src/clp/FileDescriptorReader.hpp"
#include "../src/clp/FileReader.hpp"
#include "../src/clp/FileWriter.hpp"

using clp::Array;
using clp::BufferedFileReader;
using clp::ErrorCode;
using clp::ErrorCode_EndOfFile;
using clp::ErrorCode_Success;
using clp::ErrorCode_Unsupported;
using clp::FileDescriptorReader;
using clp::FileReader;
using clp::FileWriter;
using std::make_unique;
using std::memcmp;
using std::string;
using std::string_view;

static constexpr size_t cNumAlphabets = 'z' - 'a';

namespace {
auto generate_test_file(size_t test_file_size, std::string_view test_file_path) -> void {
    // Initialize data for testing
    auto test_data = Array<char>(test_file_size);
    for (size_t i = 0; i < test_data.size(); ++i) {
        test_data.at(i) = static_cast<char>('a' + (i % (cNumAlphabets)));
    }

    // Write to test file
    FileWriter file_writer;
    file_writer.open(string{test_file_path}, FileWriter::OpenMode::CREATE_FOR_WRITING);
    file_writer.write(test_data.begin(), test_file_size);
    file_writer.close();
}

auto generate_test_input_path() -> std::string {
    return "BufferedFileReader.test";
}

auto read_and_compare_data(BufferedFileReader& test_reader, FileReader ref_reader, size_t num_bytes_to_read) -> void {
    auto read_buf = std::vector<char>(num_bytes_to_read);
    auto ref_buf = std::vector<char>(num_bytes_to_read);
    read_buf.resize(num_bytes_to_read);
    ref_buf.resize(num_bytes_to_read);
    size_t num_bytes_read{0};

    REQUIRE((ErrorCode_Success
            == test_reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read)));
    REQUIRE((num_bytes_to_read == num_bytes_read));

    REQUIRE((ErrorCode_Success
                    == ref_reader.try_read(ref_buf.data(), num_bytes_to_read, num_bytes_read)));

    REQUIRE((test_reader.get_pos() == ref_reader.get_pos()));
    REQUIRE((0 == memcmp(read_buf.data(), ref_buf.data(), num_bytes_to_read)));
}

auto test_seek(BufferedFileReader& test_reader, FileReader ref_reader, size_t seek_pos) -> void {
    REQUIRE(ErrorCode_Success == test_reader.try_seek_from_begin(seek_pos));
    REQUIRE(test_reader.get_pos() == seek_pos);

    // Advance reference reader as well
    REQUIRE(ErrorCode_Success == ref_reader.try_seek_from_begin(seek_pos));
    REQUIRE(test_reader.get_pos() == ref_reader.get_pos());
}
}

TEST_CASE("General read testing", "[BufferedFileReader]") {
    // Initialize data for testing
    size_t const test_data_size = 4L * 1024 * 1024;  // 4MB
    auto const test_file_path = generate_test_input_path();
    generate_test_file(test_data_size, test_file_path);

    size_t const base_buffer_size = BufferedFileReader::cMinBufferSize << 4;
    BufferedFileReader test_reader{make_unique<FileDescriptorReader>(test_file_path), base_buffer_size};
    FileReader ref_reader{test_file_path};

    // Read a small chunk of data;
    size_t num_bytes_to_read = base_buffer_size >> 2 + 1;
    read_and_compare_data(test_reader, ref_reader, num_bytes_to_read);

    // Read a large chunk of data, so BufferedFileReader will refill the
    // internal buffer
    num_bytes_to_read = base_buffer_size + 2;
    read_and_compare_data(test_reader, ref_reader, num_bytes_to_read);

    // Read remaining data
    num_bytes_to_read = test_data_size - test_reader.get_pos();
    read_and_compare_data(test_reader, ref_reader, num_bytes_to_read);

    // Ensure the file reaches EOF
    num_bytes_to_read = 1;
    auto read_buf = Array<char>(num_bytes_to_read);
    size_t num_bytes_read{0};
    REQUIRE(ErrorCode_EndOfFile
            == test_reader.try_read(read_buf.begin(), num_bytes_to_read, num_bytes_read));

    std::filesystem::remove(test_file_path);
}

TEST_CASE("Simple Seek without a checkpoint", "[BufferedFileReader]") {
    // Initialize data for testing
    size_t const test_data_size = 4L * 1024 * 1024;  // 4MB
    auto const test_file_path = generate_test_input_path();
    generate_test_file(test_data_size, test_file_path);

    size_t const base_buffer_size = BufferedFileReader::cMinBufferSize << 4;
    BufferedFileReader test_reader{make_unique<FileDescriptorReader>(test_file_path), base_buffer_size};
    FileReader ref_reader{test_file_path};

    // Seek to some random position
    size_t seek_pos{245};
    test_seek(test_reader, ref_reader, seek_pos);

    // Do a read with
    size_t bytes_to_read {base_buffer_size + 1};
    read_and_compare_data(test_reader, ref_reader, bytes_to_read);

    // Seek forwards to another random position
    seek_pos = 345'212;
    test_seek(test_reader, ref_reader, seek_pos);

    // Do a read again
    bytes_to_read = 4;
    read_and_compare_data(test_reader, ref_reader, bytes_to_read);

    // Ensure we can't seek backwards when there's no checkpoint
    REQUIRE(ErrorCode_Unsupported == test_reader.try_seek_from_begin(seek_pos));

    std::filesystem::remove(test_file_path);
}
    // SECTION("Simple Seek without a checkpoint") {
    //     num_bytes_to_read = base_buffer_size + 4;
    //
    //     // Seek to some random position
    //     size_t seek_pos{245};
    //     REQUIRE(ErrorCode_Success == reader.try_seek_from_begin(seek_pos));
    //     buf_pos = seek_pos;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Do a read
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + seek_pos, num_bytes_to_read));
    //
    //     // Seek forwards to another random position
    //     seek_pos = 345'212;
    //     REQUIRE(ErrorCode_Success == reader.try_seek_from_begin(seek_pos));
    //     buf_pos = seek_pos;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Do a read
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + seek_pos, num_bytes_to_read));
    //
    //     // Ensure we can't seek backwards when there's no checkpoint
    //     REQUIRE(ErrorCode_Unsupported == reader.try_seek_from_begin(seek_pos));
    // }
    //
    // SECTION("Seek with a checkpoint") {
    //     // Read some data to advance the read head
    //     num_bytes_to_read = base_buffer_size + 4;
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     auto checkpoint_pos = reader.set_checkpoint();
    //
    //     // Read some more data
    //     num_bytes_to_read = 345'212;
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     size_t highest_file_pos = reader.get_pos();
    //
    //     // Seek backwards to somewhere between the checkpoint and the read head
    //     size_t const seek_pos_1 = checkpoint_pos + 500;
    //     REQUIRE(ErrorCode_Success == reader.try_seek_from_begin(seek_pos_1));
    //     buf_pos = seek_pos_1;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Read some data
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     highest_file_pos = std::max(highest_file_pos, reader.get_pos());
    //
    //     // Ensure we can't seek to a position that's before the checkpoint
    //     REQUIRE(ErrorCode_Unsupported == reader.try_seek_from_begin(checkpoint_pos - 1));
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Seek back to the highest file pos
    //     REQUIRE(ErrorCode_Success == reader.try_seek_from_begin(highest_file_pos));
    //     buf_pos = highest_file_pos;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Do a read
    //     num_bytes_to_read = 4096;
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     highest_file_pos = reader.get_pos();
    //
    //     // Seek to somewhere between the checkpoint and latest data
    //     size_t const seek_pos_2 = (highest_file_pos + checkpoint_pos) / 2;
    //     reader.seek_from_begin(seek_pos_2);
    //     buf_pos = seek_pos_2;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Set a new checkpoint
    //     reader.set_checkpoint();
    //
    //     // Ensure we can't seek to seek_pos_1
    //     REQUIRE(ErrorCode_Unsupported == reader.try_seek_from_begin(seek_pos_1));
    //
    //     // Do a read
    //     num_bytes_to_read = 4096;
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     reader.clear_checkpoint();
    //     buf_pos = highest_file_pos;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Do a read
    //     num_bytes_to_read = base_buffer_size;
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    // }
    //
    // SECTION("Seek with delayed read") {
    //     // Advance to some random position
    //     size_t seek_pos = 45'313;
    //     REQUIRE(ErrorCode_Success == reader.try_seek_from_begin(seek_pos));
    //     buf_pos = seek_pos;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     auto checkpoint_pos = reader.set_checkpoint();
    //
    //     // Do a read
    //     num_bytes_to_read = 345'212;
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Seek to somewhere between the checkpoint and the read head
    //     seek_pos = reader.get_pos() / 2;
    //     REQUIRE(ErrorCode_Success == reader.try_seek_from_begin(seek_pos));
    //     buf_pos = seek_pos;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Do a read
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Seek close to the end of the file
    //     seek_pos = test_data_size - 500;
    //     REQUIRE(ErrorCode_Success == reader.try_seek_from_begin(seek_pos));
    //     buf_pos = seek_pos;
    //     REQUIRE(reader.get_pos() == buf_pos);
    //
    //     // Do a read
    //     num_bytes_to_read = test_data_size - seek_pos;
    //     REQUIRE(ErrorCode_Success
    //             == reader.try_read(read_buf.data(), num_bytes_to_read, num_bytes_read));
    //     REQUIRE(num_bytes_to_read == num_bytes_read);
    //     REQUIRE(0 == memcmp(read_buf.data(), test_data.data() + buf_pos, num_bytes_to_read));
    //     buf_pos += num_bytes_read;
    //     REQUIRE(reader.get_pos() == buf_pos);
    // }
    //

TEST_CASE("Test delimiter", "[BufferedFileReader]") {
    // Initialize data for testing
    size_t const test_data_size = 1L * 1024 * 1024;  // 1MB
    auto const test_file_path = generate_test_input_path();
    generate_test_file(test_data_size, test_file_path);

    size_t const reader_begin_offset = GENERATE(0, 127);

    // Instantiate BufferedFileReader and the reference FileReader from a non-zero pos
    auto fd_reader = make_unique<FileDescriptorReader>(test_file_path);
    fd_reader->seek_from_begin(reader_begin_offset);
    BufferedFileReader buffered_file_reader{std::move(fd_reader)};

    FileReader ref_file_reader{test_file_path};
    ref_file_reader.seek_from_begin(reader_begin_offset);

    // Validate a clearing a checkpoint without any reading wouldn't change the beginning offset
    buffered_file_reader.clear_checkpoint();
    REQUIRE(reader_begin_offset == buffered_file_reader.get_pos());

    // Validate that a FileReader and a BufferedFileReader return the same strings (split by
    // delimiters)
    string test_string;
    string ref_string;
    ErrorCode error_code{ErrorCode_Success};
        // The delimiter here is weird.
    auto delimiter = (char)('a' + (std::rand() % (cNumAlphabets)));
    while (true) {
        auto const ref_error_code = ref_file_reader.try_read_to_delimiter(delimiter, true, false, ref_string);
        auto const error_code
                = buffered_file_reader.try_read_to_delimiter(delimiter, true, false, test_string);
        REQUIRE(ref_error_code == error_code);
        if (ref_error_code != ErrorCode_Success) {
            break;
        }
        REQUIRE(test_string == ref_string);
    }

    std::filesystem::remove(test_file_path);
}
