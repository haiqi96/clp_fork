#include "Utils.hpp"

// C libraries
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

// C++ libraries
#include <algorithm>
#include <iostream>
#include <set>

// Boost libraries
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

// Project headers
#include "spdlog_with_specializations.hpp"
#include "string_utils.hpp"

using std::list;
using std::string;
using std::vector;

ErrorCode create_directory (const string& path, mode_t mode, bool exist_ok) {
    int retval = mkdir(path.c_str(), mode);
    if (0 != retval ) {
        if (EEXIST != errno) {
            return ErrorCode_errno;
        } else if (false == exist_ok) {
            return ErrorCode_FileExists;
        }
    }

    return ErrorCode_Success;
}

ErrorCode create_directory_structure (const string& path, mode_t mode) {
    assert(!path.empty());

    // Check if entire path already exists
    struct stat s = {};
    if (0 == stat(path.c_str(), &s)) {
        // Deepest directory exists, so can return here
        return ErrorCode_Success;
    } else if (ENOENT != errno) {
        // Unexpected error
        return ErrorCode_errno;
    }

    // Find deepest directory which exists, starting from the (2nd) deepest directory
    size_t path_end_pos = path.find_last_of('/');
    size_t last_path_end_pos = path.length();
    string dir_path;
    while (string::npos != path_end_pos) {
        if (last_path_end_pos - path_end_pos > 1) {
            dir_path.assign(path, 0, path_end_pos);
            if (0 == stat(dir_path.c_str(), &s)) {
                break;
            } else if (ENOENT != errno) {
                // Unexpected error
                return ErrorCode_errno;
            }
        }

        last_path_end_pos = path_end_pos;
        path_end_pos = path.find_last_of('/', path_end_pos - 1);
    }

    if (string::npos == path_end_pos) {
        // NOTE: Since the first path we create below contains more than one character, this assumes the path "/"
        // already exists
        path_end_pos = 0;
    }
    while (string::npos != path_end_pos) {
        path_end_pos = path.find_first_of('/', path_end_pos + 1);
        dir_path.assign(path, 0, path_end_pos);
        // Technically the directory shouldn't exist at this point in the code, but it may have been created concurrently.
        auto error_code = create_directory(dir_path, mode, true);
        if (ErrorCode_Success != error_code) {
            return error_code;
        }
    }

    return ErrorCode_Success;
}

bool get_bounds_of_next_var (const string& msg, size_t& begin_pos, size_t& end_pos) {
    const auto msg_length = msg.length();
    if (end_pos >= msg_length) {
        return false;
    }

    while (true) {
        begin_pos = end_pos;
        // Find next non-delimiter
        for (; begin_pos < msg_length; ++begin_pos) {
            if (false == is_delim(msg[begin_pos])) {
                break;
            }
        }
        if (msg_length == begin_pos) {
            // Early exit for performance
            return false;
        }

        bool contains_decimal_digit = false;
        bool contains_alphabet = false;

        // Find next delimiter
        end_pos = begin_pos;
        for (; end_pos < msg_length; ++end_pos) {
            char c = msg[end_pos];
            if (is_decimal_digit(c)) {
                contains_decimal_digit = true;
            } else if (is_alphabet(c)) {
                contains_alphabet = true;
            } else if (is_delim(c)) {
                break;
            }
        }

        // Treat token as variable if:
        // - it contains a decimal digit, or
        // - it's directly preceded by an equals sign and contains an alphabet, or
        // - it could be a multi-digit hex value
        if (contains_decimal_digit || (begin_pos > 0 && '=' == msg[begin_pos - 1] && contains_alphabet) ||
            could_be_multi_digit_hex_value(msg, begin_pos, end_pos))
        {
            break;
        }
    }

    return (msg_length != begin_pos);
}

string get_parent_directory_path (const string& path) {
    string dirname = get_unambiguous_path(path);

    size_t last_slash_pos = dirname.find_last_of('/');
    if (0 == last_slash_pos) {
        dirname = "/";
    } else if (string::npos == last_slash_pos) {
        dirname = ".";
    } else {
        dirname.resize(last_slash_pos);
    }

    return dirname;
}

string get_unambiguous_path (const string& path) {
    string unambiguous_path;
    if (path.empty()) {
        return unambiguous_path;
    }

    // Break path into components
    vector<string> path_components;
    boost::split(path_components, path, boost::is_any_of("/"), boost::token_compress_on);

    // Remove ambiguous components
    list<string> unambiguous_components;
    size_t num_components_to_ignore = 0;
    for (size_t i = path_components.size(); i-- > 0; ) {
        if (".." == path_components[i]) {
            ++num_components_to_ignore;
        } else if ("." == path_components[i] || path_components[i].empty()) {
            // Do nothing
        } else if (num_components_to_ignore > 0) {
            --num_components_to_ignore;
        } else {
            unambiguous_components.emplace_front(path_components[i]);
        }
    }

    // Assemble unambiguous path from leading slash (if any) and the unambiguous components
    if ('/' == path[0]) {
        unambiguous_path += '/';
    }
    if (!unambiguous_components.empty()) {
        unambiguous_path += boost::join(unambiguous_components, "/");
    }

    return unambiguous_path;
}

ErrorCode read_list_of_paths (const string& list_path, vector<string>& paths) {
    FileReader file_reader;
    ErrorCode error_code = file_reader.try_open(list_path);
    if (ErrorCode_Success != error_code) {
        return error_code;
    }

    // Read file
    string line;
    while (true) {
        error_code = file_reader.try_read_to_delimiter('\n', false, false, line);
        if (ErrorCode_Success != error_code) {
            break;
        }
        // Only add non-empty paths
        if (line.empty() == false) {
            paths.push_back(line);
        }
    }
    // Check for any unexpected errors
    if (ErrorCode_EndOfFile != error_code) {
        return error_code;
    }

    file_reader.close();

    return ErrorCode_Success;
}

// This return the index that's before the first token which contains a variable
size_t get_variable_front_boundary_delimiter(const std::vector<std::string>& tokens, const std::string& logtype_str) {
    enum class VarDelim {
        // NOTE: These values are used within logtypes to denote variables, so care must be taken when changing them
        NonDouble = 17,
        Double = 18,
        Length = 2,
    };

    size_t left_boundary = 0;
    for(const auto& token: tokens) {
        if (token == "*") {
            continue;
        }
        size_t found = logtype_str.find(token);
        if(found == std::string::npos) {
            SPDLOG_ERROR("ERROR, this is potentially because string in {} can be also variable dictionary value", token);
            throw;
        }
        size_t first_token_position = found;
        if(first_token_position > left_boundary) {
            left_boundary = first_token_position;
        }

        if(token.find((char)VarDelim::NonDouble) != std::string::npos || token.find((char)VarDelim::Double) != std::string::npos) {
            // This means we found a token containing a variable, we should stop.
            break;
        }
    }
    return left_boundary;
}

size_t get_variable_back_boundary_delimiter(const std::vector<std::string>& tokens, const std::string& logtype_str) {

    enum class VarDelim {
        // NOTE: These values are used within logtypes to denote variables, so care must be taken when changing them
        NonDouble = 17,
        Double = 18,
        Length = 2,
    };

    size_t right_boundary = UINT64_MAX;
    for(auto iter = tokens.rbegin(); iter != tokens.rend(); iter++) {
        const auto& token = (*iter);
        if (token == "*") {
            continue;
        }
        size_t found = logtype_str.rfind(token);
        if(found == std::string::npos) {
            SPDLOG_ERROR("SERIOUS ERROR");
            throw;
        }
        // this position is actually the first char after the first token
        size_t first_token_position = found;
        if(first_token_position < right_boundary) {
            // here we can always add the tokensize.
            right_boundary = first_token_position + token.size();
        }

        if(token.find((char)VarDelim::NonDouble) != std::string::npos || token.find((char)VarDelim::Double) != std::string::npos) {
            // This means we found a token containing a variable, we should stop.
            break;
        }
    }
    // This is the begin of the token, so the actual token is not included.
    return right_boundary;
}

std::vector<std::string> split_wildcard(const std::string& input_str) {
    size_t pos = 0;
    std::vector<std::string> return_res;
    std::string token;
    std::string delim = "*";

    auto start = 0U;
    auto end = input_str.find(delim);
    while (end != std::string::npos)
    {
        std::string matched = input_str.substr(start, end - start);
        if(!matched.empty()){
            return_res.push_back(matched);
        }
        return_res.push_back(delim);
        start = end + delim.length();
        end = input_str.find(delim, start);
    }
    // we should never see this, because the last token is always a * due to the natural of the query
    if(start < input_str.size()) {
        return_res.push_back(input_str.substr(start, end));
    }
    return return_res;
}