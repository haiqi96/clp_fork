#include "Grep.hpp"

// C++ libraries
#include <algorithm>

// Project headers
#include "compressor_frontend/Constants.hpp"
#include "EncodedVariableInterpreter.hpp"
#include "StringReader.hpp"
#include "Utils.hpp"

#include <iostream>

// msgpack
#include <msgpack.hpp>

#include "networking/socket_utils.hpp"
#include "streaming_archive/reader/GLT/GLTArchive.hpp"
#include "streaming_archive/reader/GLT/GLTMessage.hpp"

using std::map;
using std::vector;
using std::map;
using streaming_archive::reader::Archive;
using streaming_archive::reader::File;
using streaming_archive::reader::clp::CLPArchive;
using streaming_archive::reader::glt::GLTArchive;
using streaming_archive::reader::clp::CLPFile;
using streaming_archive::reader::Message;
using streaming_archive::reader::glt::GLTMessage;
// Local types
enum class SubQueryMatchabilityResult {
    MayMatch, // The subquery might match a message
    WontMatch, // The subquery has no chance of matching a message
    SupercedesAllSubQueries // The subquery will cause all messages to be matched
};

// Class representing a token in a query. It is used to interpret a token in user's search string.
class QueryToken {
public:
    // Constructors
    QueryToken (const string& query_string, size_t begin_pos, size_t end_pos, bool is_var);

    // Methods
    bool cannot_convert_to_non_dict_var () const;
    bool contains_wildcards () const;
    bool has_greedy_wildcard_in_middle () const;
    bool has_prefix_greedy_wildcard () const;
    bool has_suffix_greedy_wildcard () const;
    bool is_ambiguous_token () const;
    bool is_float_var () const;
    bool is_int_var () const;
    bool is_var () const;
    bool is_wildcard () const;

    size_t get_begin_pos () const;
    size_t get_end_pos () const;
    const string& get_value () const;

    bool change_to_next_possible_type ();

private:
    // Types
    // Type for the purpose of generating different subqueries. E.g., if a token is of type DictOrIntVar, it would generate a different subquery than
    // if it was of type Logtype.
    enum class Type {
        Wildcard,
        // Ambiguous indicates the token can be more than one of the types listed below
        Ambiguous,
        Logtype,
        DictionaryVar,
        FloatVar,
        IntVar
    };

    // Variables
    bool m_cannot_convert_to_non_dict_var;
    bool m_contains_wildcards;
    bool m_has_greedy_wildcard_in_middle;
    bool m_has_prefix_greedy_wildcard;
    bool m_has_suffix_greedy_wildcard;

    size_t m_begin_pos;
    size_t m_end_pos;
    string m_value;

    // Type if variable has unambiguous type
    Type m_type;
    // Types if variable type is ambiguous
    vector<Type> m_possible_types;
    // Index of the current possible type selected for generating a subquery
    size_t m_current_possible_type_ix;
};

QueryToken::QueryToken (const string& query_string, const size_t begin_pos, const size_t end_pos,
                        const bool is_var) : m_current_possible_type_ix(0)
{
    m_begin_pos = begin_pos;
    m_end_pos = end_pos;
    m_value.assign(query_string, m_begin_pos, m_end_pos - m_begin_pos);

    // Set wildcard booleans and determine type
    if ("*" == m_value) {
        m_has_prefix_greedy_wildcard = true;
        m_has_suffix_greedy_wildcard = false;
        m_has_greedy_wildcard_in_middle = false;
        m_contains_wildcards = true;
        m_type = Type::Wildcard;
    } else {
        m_has_prefix_greedy_wildcard = ('*' == m_value[0]);
        m_has_suffix_greedy_wildcard = ('*' == m_value[m_value.length() - 1]);

        m_has_greedy_wildcard_in_middle = false;
        for (size_t i = 1; i < m_value.length() - 1; ++i) {
            if ('*' == m_value[i]) {
                m_has_greedy_wildcard_in_middle = true;
                break;
            }
        }

        m_contains_wildcards = (m_has_prefix_greedy_wildcard || m_has_suffix_greedy_wildcard ||
                                m_has_greedy_wildcard_in_middle);

        if (!is_var) {
            if (!m_contains_wildcards) {
                m_type = Type::Logtype;
            } else {
                m_type = Type::Ambiguous;
                m_possible_types.push_back(Type::Logtype);
                m_possible_types.push_back(Type::IntVar);
                m_possible_types.push_back(Type::FloatVar);
                m_possible_types.push_back(Type::DictionaryVar);
            }
        } else {
            string value_without_wildcards = m_value;
            if (m_has_prefix_greedy_wildcard) {
                value_without_wildcards = value_without_wildcards.substr(1);
            }
            if (m_has_suffix_greedy_wildcard) {
                value_without_wildcards.resize(value_without_wildcards.length() - 1);
            }

            encoded_variable_t encoded_var;
            bool converts_to_non_dict_var = false;
            bool converts_to_int = EncodedVariableInterpreter::convert_string_to_representable_integer_var(value_without_wildcards, encoded_var);
            bool converts_to_double = false;
            if(!converts_to_int) {
                converts_to_double = EncodedVariableInterpreter::convert_string_to_representable_float_var(value_without_wildcards, encoded_var);
            }
            if (converts_to_int || converts_to_double)
            {
                converts_to_non_dict_var = true;
            }

            if (!converts_to_non_dict_var) {
                // Dictionary variable
                // HACK: Actually this is incorrect, because it's possible user enters 23412*34 aiming to
                // match 23412.34. This should be an ambiguous type.
                // Ideally, while parsing the variable, there should be a similar boolean as
                // is_var that track if all chars in the token are integer.
                // But now I will just leave it in consistent with CLG.
                m_type = Type::DictionaryVar;
                m_cannot_convert_to_non_dict_var = true;
            } else {
                if (converts_to_int) {
                    if (m_has_prefix_greedy_wildcard || m_has_suffix_greedy_wildcard) {
                        // if there is prefix or suffix wildcard, then the integer could be
                        // a part of the float, or dictionary variable
                        m_type = Type::Ambiguous;
                        m_possible_types.push_back(Type::IntVar);
                        m_possible_types.push_back(Type::FloatVar);
                        m_possible_types.push_back(Type::DictionaryVar);
                    } else {
                        m_type = Type::IntVar;
                        m_possible_types.push_back(Type::IntVar);
                    }
                } else {
                    if (m_has_prefix_greedy_wildcard || m_has_suffix_greedy_wildcard) {
                        // if there is prefix or suffix wildcard, then the float could be
                        // a part of a dictionary variable
                        m_type = Type::Ambiguous;
                        m_possible_types.push_back(Type::FloatVar);
                        m_possible_types.push_back(Type::DictionaryVar);
                    } else {
                        m_type = Type::FloatVar;
                        m_possible_types.push_back(Type::FloatVar);
                    }
                }
                m_cannot_convert_to_non_dict_var = false;
            }
        }
    }
}

bool QueryToken::cannot_convert_to_non_dict_var () const {
    return m_cannot_convert_to_non_dict_var;
}

bool QueryToken::contains_wildcards () const {
    return m_contains_wildcards;
}

bool QueryToken::has_greedy_wildcard_in_middle () const {
    return m_has_greedy_wildcard_in_middle;
}

bool QueryToken::has_prefix_greedy_wildcard () const {
    return m_has_prefix_greedy_wildcard;
}

bool QueryToken::has_suffix_greedy_wildcard () const {
    return m_has_suffix_greedy_wildcard;
}

bool QueryToken::is_ambiguous_token () const {
    return Type::Ambiguous == m_type;
}

bool QueryToken::is_float_var () const {
    Type type;
    if (Type::Ambiguous == m_type) {
        type = m_possible_types[m_current_possible_type_ix];
    } else {
        type = m_type;
    }
    return Type::FloatVar == type;
}

bool QueryToken::is_int_var () const {
    Type type;
    if (Type::Ambiguous == m_type) {
        type = m_possible_types[m_current_possible_type_ix];
    } else {
        type = m_type;
    }
    return Type::IntVar == type;
}

bool QueryToken::is_var () const {
    Type type;
    if (Type::Ambiguous == m_type) {
        type = m_possible_types[m_current_possible_type_ix];
    } else {
        type = m_type;
    }
    return (Type::IntVar == type || Type::FloatVar == type || Type::DictionaryVar == type);
}

bool QueryToken::is_wildcard () const {
    return Type::Wildcard == m_type;
}

size_t QueryToken::get_begin_pos () const {
    return m_begin_pos;
}

size_t QueryToken::get_end_pos () const {
    return m_end_pos;
}

const string& QueryToken::get_value () const {
    return m_value;
}

bool QueryToken::change_to_next_possible_type () {
    if (m_current_possible_type_ix < m_possible_types.size() - 1) {
        ++m_current_possible_type_ix;
        return true;
    } else {
        m_current_possible_type_ix = 0;
        return false;
    }
}

// Local prototypes
/**
 * Process a QueryToken that is definitely a variable
 * @param query_token
 * @param archive
 * @param ignore_case
 * @param sub_query
 * @param logtype
 * @return true if this token might match a message, false otherwise
 */
static bool process_var_token (const QueryToken& query_token, const Archive& archive, bool ignore_case, SubQuery& sub_query, string& logtype);
/**
 * Finds a message matching the given query
 * @param query
 * @param archive
 * @param matching_sub_query
 * @param compressed_file
 * @param compressed_msg
 * @return true on success, false otherwise
 */
static bool find_matching_message (const Query& query, CLPArchive& archive, const SubQuery*& matching_sub_query, CLPFile& compressed_file, Message& compressed_msg);
/**
 * Generates logtypes and variables for subquery
 * @param archive
 * @param processed_search_string
 * @param query_tokens
 * @param ignore_case
 * @param sub_query
 * @return SubQueryMatchabilityResult::SupercedesAllSubQueries
 * @return SubQueryMatchabilityResult::WontMatch
 * @return SubQueryMatchabilityResult::MayMatch
 */
static SubQueryMatchabilityResult generate_logtypes_and_vars_for_subquery (const Archive& archive, string& processed_search_string,
                                                                           vector<QueryToken>& query_tokens, bool ignore_case, SubQuery& sub_query);

static bool process_var_token (const QueryToken& query_token, const Archive& archive, bool ignore_case, SubQuery& sub_query, string& logtype) {
    // Even though we may have a precise variable, we still fallback to decompressing to ensure that it is in the right place in the message
    sub_query.mark_wildcard_match_required();

    // Create QueryVar corresponding to token
    if (!query_token.contains_wildcards()) {
        if (EncodedVariableInterpreter::encode_and_search_dictionary(query_token.get_value(), archive.get_var_dictionary(), ignore_case, logtype,
                                                                     sub_query) == false)
        {
            // Variable doesn't exist in dictionary
            return false;
        }
    } else {
        if (query_token.has_prefix_greedy_wildcard()) {
            logtype += '*';
        }

        if (query_token.is_float_var()) {
            LogTypeDictionaryEntry::add_float_var(logtype);
        } else if (query_token.is_int_var()) {
            LogTypeDictionaryEntry::add_int_var(logtype);
        } else {
            LogTypeDictionaryEntry::add_dict_var(logtype);

            if (query_token.cannot_convert_to_non_dict_var()) {
                // Must be a dictionary variable, so search variable dictionary
                if (!EncodedVariableInterpreter::wildcard_search_dictionary_and_get_encoded_matches(query_token.get_value(), archive.get_var_dictionary(),
                                                                                                    ignore_case, sub_query))
                {
                    // Variable doesn't exist in dictionary
                    return false;
                }
            }
        }

        if (query_token.has_suffix_greedy_wildcard()) {
            logtype += '*';
        }
    }

    return true;
}

static bool find_matching_message (const Query& query, CLPArchive& archive, const SubQuery*& matching_sub_query, CLPFile& compressed_file, Message& compressed_msg) {
    if (query.contains_sub_queries()) {
        matching_sub_query = archive.find_message_matching_query(compressed_file, query, compressed_msg);
        if (nullptr == matching_sub_query) {
            return false;
        }
    } else if (query.get_search_begin_timestamp() > cEpochTimeMin || query.get_search_end_timestamp() < cEpochTimeMax) {
        bool found_msg = archive.find_message_in_time_range(compressed_file, query.get_search_begin_timestamp(), query.get_search_end_timestamp(),
                                                            compressed_msg);
        if (!found_msg) {
            return false;
        }
    } else {
        bool read_successful = archive.get_next_message(compressed_file, compressed_msg);
        if (!read_successful) {
            return false;
        }
    }

    return true;
}

SubQueryMatchabilityResult generate_logtypes_and_vars_for_subquery (const Archive& archive, string& processed_search_string, vector<QueryToken>& query_tokens,
                                                                    bool ignore_case, SubQuery& sub_query)
{
    size_t last_token_end_pos = 0;
    string logtype;
    for (const auto& query_token : query_tokens) {
        // Append from end of last token to beginning of this token, to logtype
        logtype.append(processed_search_string, last_token_end_pos, query_token.get_begin_pos() - last_token_end_pos);
        last_token_end_pos = query_token.get_end_pos();

        if (query_token.is_wildcard()) {
            logtype += '*';
        } else if (query_token.has_greedy_wildcard_in_middle()) {
            // Fallback to decompression + wildcard matching for now to avoid handling queries where the pieces of the token on either side of each wildcard
            // need to be processed as ambiguous tokens
            sub_query.mark_wildcard_match_required();
            if (!query_token.is_var()) {
                logtype += '*';
            } else {
                logtype += '*';
                LogTypeDictionaryEntry::add_dict_var(logtype);
                logtype += '*';
            }
        } else {
            if (!query_token.is_var()) {
                logtype += query_token.get_value();
            } else if (!process_var_token(query_token, archive, ignore_case, sub_query, logtype)) {
                return SubQueryMatchabilityResult::WontMatch;
            }
        }
    }

    if (last_token_end_pos < processed_search_string.length()) {
        // Append from end of last token to end
        logtype.append(processed_search_string, last_token_end_pos, string::npos);
        last_token_end_pos = processed_search_string.length();
    }

    if ("*" == logtype) {
        // Logtype will match all messages
        return SubQueryMatchabilityResult::SupercedesAllSubQueries;
    }
    // note: onthing to be careful is that a string is connected with a wildcard, things can become complicated.
    // because we don't know whether that string is a dictionary type or logtype.
    // for example: "*\021 reply*"
    sub_query.m_tokens = split_wildcard(logtype);

    // Find matching logtypes
    std::unordered_set<const LogTypeDictionaryEntry*> possible_logtype_entries;
    archive.get_logtype_dictionary().get_entries_matching_wildcard_string(logtype, ignore_case, possible_logtype_entries);
    if (possible_logtype_entries.empty()) {
        return SubQueryMatchabilityResult::WontMatch;
    }
    sub_query.set_possible_logtypes(possible_logtype_entries);

    // Calculate the IDs of the segments that may contain results for the sub-query now that we've calculated the matching logtypes and variables
    sub_query.calculate_ids_of_matching_segments();

    return SubQueryMatchabilityResult::MayMatch;
}

bool Grep::process_raw_query (const Archive& archive, const string& search_string, epochtime_t search_begin_ts, epochtime_t search_end_ts, bool ignore_case,
                              Query& query, compressor_frontend::lexers::ByteLexer& forward_lexer, compressor_frontend::lexers::ByteLexer& reverse_lexer,
                              bool use_heuristic)
{
    // Set properties which require no processing
    query.set_search_begin_timestamp(search_begin_ts);
    query.set_search_end_timestamp(search_end_ts);
    query.set_ignore_case(ignore_case);

    // Add prefix and suffix '*' to make the search a sub-string match
    string processed_search_string = "*";
    processed_search_string += search_string;
    processed_search_string += '*';

    // Clean-up search string
    processed_search_string = clean_up_wildcard_search_string(processed_search_string);
    query.set_search_string(processed_search_string);

    // Replace non-greedy wildcards with greedy wildcards since we currently have no support for searching compressed files with non-greedy wildcards
    std::replace(processed_search_string.begin(), processed_search_string.end(), '?', '*');
    // Clean-up in case any instances of "?*" or "*?" were changed into "**"
    processed_search_string = clean_up_wildcard_search_string(processed_search_string);

    // Split search_string into tokens with wildcards
    vector<QueryToken> query_tokens;
    size_t begin_pos = 0;
    size_t end_pos = 0;
    bool is_var;
    if (use_heuristic) {
        while (get_bounds_of_next_potential_var(processed_search_string, begin_pos, end_pos, is_var)) {
            query_tokens.emplace_back(processed_search_string, begin_pos, end_pos, is_var);
        }
    } else {
        while (get_bounds_of_next_potential_var(processed_search_string, begin_pos, end_pos, is_var, forward_lexer, reverse_lexer)) {
            query_tokens.emplace_back(processed_search_string, begin_pos, end_pos, is_var);
        }
    }

    // Get pointers to all ambiguous tokens. Exclude tokens with wildcards in the middle since we fall-back to decompression + wildcard matching for those.
    vector<QueryToken*> ambiguous_tokens;
    for (auto& query_token : query_tokens) {
        if (!query_token.has_greedy_wildcard_in_middle() && query_token.is_ambiguous_token()) {
            ambiguous_tokens.push_back(&query_token);
        }
    }

    // Generate a sub-query for each combination of ambiguous tokens
    // E.g., if there are two ambiguous tokens each of which could be a logtype or variable, we need to create:
    // - (token1 as logtype) (token2 as logtype)
    // - (token1 as logtype) (token2 as var)
    // - (token1 as var) (token2 as logtype)
    // - (token1 as var) (token2 as var)
    SubQuery sub_query;
    string logtype;
    bool type_of_one_token_changed = true;
    while (type_of_one_token_changed) {
        sub_query.clear();

        // Compute logtypes and variables for query
        auto matchability = generate_logtypes_and_vars_for_subquery(archive, processed_search_string, query_tokens, query.get_ignore_case(), sub_query);
        switch (matchability) {
            case SubQueryMatchabilityResult::SupercedesAllSubQueries:
                // Clear all sub-queries since they will be superceded by this sub-query
                query.clear_sub_queries();

                // Since other sub-queries will be superceded by this one, we can stop processing now
                return true;
            case SubQueryMatchabilityResult::MayMatch:
                query.add_sub_query(sub_query);
                break;
            case SubQueryMatchabilityResult::WontMatch:
            default:
                // Do nothing
                break;
        }

        // Update combination of ambiguous tokens
        type_of_one_token_changed = false;
        for (auto* ambiguous_token : ambiguous_tokens) {
            if (ambiguous_token->change_to_next_possible_type()) {
                type_of_one_token_changed = true;
                break;
            }
        }
    }

    return query.contains_sub_queries();
}

bool Grep::get_bounds_of_next_potential_var (const string& value, size_t& begin_pos, size_t& end_pos, bool& is_var) {
    const auto value_length = value.length();
    if (end_pos >= value_length) {
        return false;
    }

    is_var = false;
    bool contains_wildcard = false;
    while (false == is_var && false == contains_wildcard && begin_pos < value_length) {
        // Start search at end of last token
        begin_pos = end_pos;

        // Find next wildcard or non-delimiter
        bool is_escaped = false;
        for (; begin_pos < value_length; ++begin_pos) {
            char c = value[begin_pos];

            if (is_escaped) {
                is_escaped = false;

                if (false == is_delim(c)) {
                    // Found escaped non-delimiter, so reverse the index to retain the escape character
                    --begin_pos;
                    break;
                }
            } else if ('\\' == c) {
                // Escape character
                is_escaped = true;
            } else {
                if (is_wildcard(c)) {
                    contains_wildcard = true;
                    break;
                }
                if (false == is_delim(c)) {
                    break;
                }
            }
        }

        bool contains_decimal_digit = false;
        bool contains_alphabet = false;

        // Find next delimiter
        is_escaped = false;
        end_pos = begin_pos;
        for (; end_pos < value_length; ++end_pos) {
            char c = value[end_pos];

            if (is_escaped) {
                is_escaped = false;

                if (is_delim(c)) {
                    // Found escaped delimiter, so reverse the index to retain the escape character
                    --end_pos;
                    break;
                }
            } else if ('\\' == c) {
                // Escape character
                is_escaped = true;
            } else {
                if (is_wildcard(c)) {
                    contains_wildcard = true;
                } else if (is_delim(c)) {
                    // Found delimiter that's not also a wildcard
                    break;
                }
            }

            if (is_decimal_digit(c)) {
                contains_decimal_digit = true;
            } else if (is_alphabet(c)) {
                contains_alphabet = true;
            }
        }

        // Treat token as a definite variable if:
        // - it contains a decimal digit, or
        // - it could be a multi-digit hex value, or
        // - it's directly preceded by an equals sign and contains an alphabet without a wildcard between the equals sign and the first alphabet of the token
        if (contains_decimal_digit || could_be_multi_digit_hex_value(value, begin_pos, end_pos)) {
            is_var = true;
        } else if (begin_pos > 0 && '=' == value[begin_pos - 1] && contains_alphabet) {
            // Find first alphabet or wildcard in token
            is_escaped = false;
            bool found_wildcard_before_alphabet = false;
            for (auto i = begin_pos; i < end_pos; ++i) {
                auto c = value[i];

                if (is_escaped) {
                    is_escaped = false;

                    if (is_alphabet(c)) {
                        break;
                    }
                } else if ('\\' == c) {
                    // Escape character
                    is_escaped = true;
                } else if (is_wildcard(c)) {
                    found_wildcard_before_alphabet = true;
                    break;
                }
            }

            if (false == found_wildcard_before_alphabet) {
                is_var = true;
            }
        }
    }

    return (value_length != begin_pos);
}

bool
Grep::get_bounds_of_next_potential_var (const string& value, size_t& begin_pos, size_t& end_pos, bool& is_var,
                                        compressor_frontend::lexers::ByteLexer& forward_lexer, compressor_frontend::lexers::ByteLexer& reverse_lexer) {
    const size_t value_length = value.length();
    if (end_pos >= value_length) {
        return false;
    }

    is_var = false;
    bool contains_wildcard = false;
    while (false == is_var && false == contains_wildcard && begin_pos < value_length) {
        // Start search at end of last token
        begin_pos = end_pos;

        // Find variable begin or wildcard
        bool is_escaped = false;
        for (; begin_pos < value_length; ++begin_pos) {
            char c = value[begin_pos];

            if (is_escaped) {
                is_escaped = false;

                if(false == forward_lexer.is_delimiter(c)) {
                    // Found escaped non-delimiter, so reverse the index to retain the escape character
                    --begin_pos;
                    break;
                }
            } else if ('\\' == c) {
                // Escape character
                is_escaped = true;
            } else {
                if (is_wildcard(c)) {
                    contains_wildcard = true;
                    break;
                }
                if (false == forward_lexer.is_delimiter(c)) {
                    break;
                }
            }
        }

        // Find next delimiter
        is_escaped = false;
        end_pos = begin_pos;
        for (; end_pos < value_length; ++end_pos) {
            char c = value[end_pos];

            if (is_escaped) {
                is_escaped = false;

                if (forward_lexer.is_delimiter(c)) {
                    // Found escaped delimiter, so reverse the index to retain the escape character
                    --end_pos;
                    break;
                }
            } else if ('\\' == c) {
                // Escape character
                is_escaped = true;
            } else {
                if (is_wildcard(c)) {
                    contains_wildcard = true;
                } else if (forward_lexer.is_delimiter(c)) {
                    // Found delimiter that's not also a wildcard
                    break;
                }
            }
        }

        if (end_pos > begin_pos) {
            bool has_prefix_wildcard = ('*' == value[begin_pos]) || ('?' == value[begin_pos]);
            bool has_suffix_wildcard = ('*' == value[end_pos - 1]) || ('?' == value[begin_pos]);;
            bool has_wildcard_in_middle = false;
            for (size_t i = begin_pos + 1; i < end_pos - 1; ++i) {
                if (('*' == value[i] || '?' == value[i]) && value[i - 1] != '\\') {
                    has_wildcard_in_middle = true;
                    break;
                }
            }
            if (has_wildcard_in_middle || (has_prefix_wildcard && has_suffix_wildcard)) {
                // DO NOTHING
            } else if (has_suffix_wildcard) { //asdsas*
                StringReader stringReader;
                stringReader.open(value.substr(begin_pos, end_pos - begin_pos - 1));
                forward_lexer.reset(stringReader);
                compressor_frontend::Token token = forward_lexer.scan_with_wildcard(value[end_pos - 1]);
                if (token.m_type_ids->at(0) != (int) compressor_frontend::SymbolID::TokenUncaughtStringID &&
                    token.m_type_ids->at(0) != (int) compressor_frontend::SymbolID::TokenEndID) {
                    is_var = true;
                }
            } else if (has_prefix_wildcard) { // *asdas
                std::string value_reverse = value.substr(begin_pos + 1, end_pos - begin_pos - 1);
                std::reverse(value_reverse.begin(), value_reverse.end());
                StringReader stringReader;
                stringReader.open(value_reverse);
                reverse_lexer.reset(stringReader);
                compressor_frontend::Token token = reverse_lexer.scan_with_wildcard(value[begin_pos]);
                if (token.m_type_ids->at(0) != (int) compressor_frontend::SymbolID::TokenUncaughtStringID &&
                    token.m_type_ids->at(0) != (int)compressor_frontend::SymbolID::TokenEndID) {
                    is_var = true;
                }
            } else { // no wildcards
                StringReader stringReader;
                stringReader.open(value.substr(begin_pos, end_pos - begin_pos));
                forward_lexer.reset(stringReader);
                compressor_frontend::Token token = forward_lexer.scan();
                if (token.m_type_ids->at(0) != (int) compressor_frontend::SymbolID::TokenUncaughtStringID &&
                    token.m_type_ids->at(0) != (int) compressor_frontend::SymbolID::TokenEndID) {
                    is_var = true;
                }
            }
        }
    }
    return (value_length != begin_pos);
}

void Grep::calculate_sub_queries_relevant_to_file (const File& compressed_file, vector<Query>& queries) {
    for (auto& query : queries) {
        query.make_sub_queries_relevant_to_segment(compressed_file.get_segment_id());
    }
}

// Handle the case where the processed search string is a wildcard (Note this doesn't guarantee the original search string is a wildcard)
// Return all messages as long as they fall into the time range
size_t Grep::output_message_in_segment_within_time_range (const Query& query, size_t limit, GLTArchive& archive, OutputFunc output_func, void* output_func_arg) {
    size_t num_matches = 0;

    GLTMessage compressed_msg;
    string decompressed_msg;

    // Get the correct order of looping through logtypes
    const auto& logtype_order = archive.get_table_manager().get_single_order();
    for(const auto& logtype_id : logtype_order) {
        archive.get_table_manager().load_single_table(logtype_id);
        archive.get_table_manager().load_all();
        auto num_vars = archive.get_logtype_dictionary().get_entry(logtype_id).get_num_vars();
        compressed_msg.resize_var(num_vars);
        compressed_msg.set_logtype_id(logtype_id);
        while(num_matches < limit) {
            // Find matching message
            bool found_message = archive.get_next_message_with_logtype(compressed_msg);
            if (!found_message) {
                break;
            }
            if(!query.timestamp_is_in_search_time_range(compressed_msg.get_ts_in_milli())) {
                continue;
            }
            bool decompress_successful = archive.decompress_message_with_fixed_timestamp_pattern(compressed_msg, decompressed_msg);
            if (!decompress_successful) {
                break;
            }
            // Perform wildcard match if required
            // In this branch, subqueries should not exist
            // So just check if the search string is not a match-all
            if (query.search_string_matches_all() == false)
            {
                bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(), query.get_ignore_case() == false);
                if (!matched) {
                    continue;
                }
            }
            std::string orig_file_path = archive.get_file_name(compressed_msg.get_file_id());
            // Print match
            output_func(orig_file_path, compressed_msg, decompressed_msg, output_func_arg);
            ++num_matches;
        }
        archive.get_table_manager().close_single_table();
    }
    return num_matches;
}

size_t Grep::output_message_in_combined_segment_within_time_range (const Query& query, size_t limit, GLTArchive& archive, OutputFunc output_func, void* output_func_arg) {
    size_t num_matches = 0;

    GLTMessage compressed_msg;
    string decompressed_msg;
    size_t combined_table_count = archive.get_table_manager().get_combined_table_count();
    const auto& combined_logtype_order = archive.get_table_manager().get_combined_order();
    for(size_t table_ix = 0; table_ix < combined_table_count; table_ix++) {

        // load the combined table
        archive.get_table_manager().open_combined_table(table_ix);
        const auto& logtype_order = combined_logtype_order.at(table_ix);

        for(const auto& logtype_id : logtype_order) {
            // load the logtype id
            archive.get_table_manager().open_combined_logtype_table(logtype_id);
            auto num_vars = archive.get_logtype_dictionary().get_entry(logtype_id).get_num_vars();
            compressed_msg.resize_var(num_vars);
            compressed_msg.set_logtype_id(logtype_id);
            while(num_matches < limit) {
                // Find matching message
                bool found_message = archive.get_table_manager().m_combined_table.get_next_full_row(compressed_msg);
                if (!found_message) {
                    break;
                }
                if(!query.timestamp_is_in_search_time_range(compressed_msg.get_ts_in_milli())) {
                    continue;
                }
                bool decompress_successful = archive.decompress_message_with_fixed_timestamp_pattern(compressed_msg, decompressed_msg);
                if (!decompress_successful) {
                    break;
                }
                // Perform wildcard match if required
                // In this execution branch, subqueries should not exist
                // So just check if the search string is not a match-all
                if (query.search_string_matches_all() == false)
                {
                    bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(), query.get_ignore_case() == false);
                    if (!matched) {
                        continue;
                    }
                }
                std::string orig_file_path = archive.get_file_name(compressed_msg.get_file_id());
                // Print match
                output_func(orig_file_path, compressed_msg, decompressed_msg, output_func_arg);
                ++num_matches;
            }
            archive.get_table_manager().m_combined_table.close_logtype_table();
        }
        archive.get_table_manager().close_combined_table();
    }
    return num_matches;
}

size_t Grep::search_segment_all_columns_and_output (const std::vector<LogtypeQueries>& queries, const Query& query, size_t limit, GLTArchive& archive, OutputFunc output_func, void* output_func_arg) {
    size_t num_matches = 0;

    GLTMessage compressed_msg;
    string decompressed_msg;

    // Go through each logtype
    for(const auto& query_for_logtype: queries) {
        size_t logtype_matches = 0;
        // preload the data
        auto logtype_id = query_for_logtype.m_logtype_id;
        const auto& sub_queries = query_for_logtype.m_queries;
        archive.get_table_manager().load_single_table(logtype_id);
        archive.get_table_manager().load_all();
        auto num_vars = archive.get_logtype_dictionary().get_entry(logtype_id).get_num_vars();
        compressed_msg.resize_var(num_vars);
        compressed_msg.set_logtype_id(logtype_id);

        while(num_matches < limit) {
            // Find matching message
            bool required_wild_card = false;
            bool found_matched = archive.find_message_matching_with_logtype_query(sub_queries,compressed_msg, required_wild_card, query);
            if (found_matched == false) {
                break;
            }
            // Decompress match
            bool decompress_successful = archive.decompress_message_with_fixed_timestamp_pattern(compressed_msg, decompressed_msg);
            if (!decompress_successful) {
                break;
            }

            // Perform wildcard match if required
            // Check if:
            // - Sub-query requires wildcard match, or
            // - no subqueries exist and the search string is not a match-all
            if ((query.contains_sub_queries() && required_wild_card) ||
                (query.contains_sub_queries() == false && query.search_string_matches_all() == false)) {
                bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                                     query.get_ignore_case() == false);
                if (!matched) {
                    continue;
                }
            }
            std::string orig_file_path = archive.get_file_name(compressed_msg.get_file_id());
            // Print match
            output_func(orig_file_path, compressed_msg, decompressed_msg, output_func_arg);
            ++logtype_matches;
        }
        archive.get_table_manager().close_single_table();
        num_matches += logtype_matches;
    }

    return num_matches;
}

size_t Grep::search_segment_optimized_and_output (const std::vector<LogtypeQueries>& queries, const Query& query, size_t limit, GLTArchive& archive, OutputFunc output_func, void* output_func_arg) {
    size_t num_matches = 0;

    GLTMessage compressed_msg;
    string decompressed_msg;

    // Go through each logtype
    for(const auto& query_for_logtype: queries) {
        // preload the data
        auto logtype_id = query_for_logtype.m_logtype_id;
        const auto& sub_queries = query_for_logtype.m_queries;
        archive.get_table_manager().load_single_table(logtype_id);

        size_t left_boundary, right_boundary;
        Grep::get_boundaries(sub_queries, left_boundary, right_boundary);

        // load timestamps and columns that fall into the ranges.
        archive.get_table_manager().load_ts();
        archive.get_table_manager().load_partial_columns(left_boundary, right_boundary);

        auto num_vars = archive.get_logtype_dictionary().get_entry(logtype_id).get_num_vars();

        std::vector<size_t> matched_row_ix;
        std::vector<bool> wildcard_required;
        // Find matching message
        archive.find_message_matching_with_logtype_query_optimized(sub_queries, wildcard_required, query, matched_row_ix);

        size_t num_potential_matches = matched_row_ix.size();
        if(num_potential_matches != 0) {
            // Decompress match
            std::vector<epochtime_t> loaded_ts(num_potential_matches);
            std::vector<file_id_t> loaded_file_id (num_potential_matches);
            std::vector<encoded_variable_t> loaded_vars (num_potential_matches * num_vars);
            archive.get_table_manager().m_single_table.load_remaining_data_into_vec(loaded_ts, loaded_file_id, loaded_vars, matched_row_ix);
            num_matches += archive.decompress_messages_and_output(logtype_id, loaded_ts, loaded_file_id, loaded_vars, wildcard_required, query);
        }
        archive.get_table_manager().close_single_table();
    }

    return num_matches;
}

size_t Grep::search_combined_table_and_output (combined_table_id_t table_id, const std::vector<LogtypeQueries>& queries, const Query& query, size_t limit, GLTArchive& archive, OutputFunc output_func, void* output_func_arg) {
    size_t num_matches = 0;

    GLTMessage compressed_msg;
    string decompressed_msg;

    archive.get_table_manager().open_combined_table(table_id);
    for(const auto& iter: queries) {
        logtype_dictionary_id_t logtype_id = iter.m_logtype_id;
        archive.get_table_manager().open_combined_logtype_table(logtype_id);

        const auto& queries_by_logtype = iter.m_queries;

        // Initialize message
        auto num_vars = archive.get_logtype_dictionary().get_entry(logtype_id).get_num_vars();
        compressed_msg.resize_var(num_vars);
        compressed_msg.set_logtype_id(logtype_id);

        size_t left_boundary, right_boundary;
        Grep::get_boundaries(queries_by_logtype, left_boundary, right_boundary);

        bool required_wild_card;
        while(num_matches < limit) {
            // Find matching message
            bool found_matched = archive.find_message_matching_with_logtype_query_from_combined(queries_by_logtype,compressed_msg, required_wild_card, query, left_boundary, right_boundary);
            if (found_matched == false) {
                break;
            }
            // Decompress match
            bool decompress_successful = archive.decompress_message_with_fixed_timestamp_pattern(compressed_msg, decompressed_msg);
            if (!decompress_successful) {
                break;
            }

            // Perform wildcard match if required
            // Check if:
            // - Sub-query requires wildcard match, or
            // - no subqueries exist and the search string is not a match-all
            if ((query.contains_sub_queries() && required_wild_card) ||
                (query.contains_sub_queries() == false && query.search_string_matches_all() == false)) {
                bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                                     query.get_ignore_case() == false);
                if (!matched) {
                    continue;
                }
            }
            std::string orig_file_path = archive.get_file_name(compressed_msg.get_file_id());
            // Print match
            output_func(orig_file_path, compressed_msg, decompressed_msg, output_func_arg);
            ++num_matches;
        }
        archive.get_table_manager().m_combined_table.close_logtype_table();
    }
    archive.get_table_manager().close_combined_table();
    return num_matches;
}

size_t Grep::search_and_output (const Query& query, size_t limit, CLPArchive& archive, CLPFile& compressed_file, OutputFunc output_func, void* output_func_arg) {
    size_t num_matches = 0;

    Message compressed_msg;
    string decompressed_msg;
    const string& orig_file_path = compressed_file.get_orig_path();
    while (num_matches < limit) {
        // Find matching message
        const SubQuery* matching_sub_query = nullptr;
        if (find_matching_message(query, archive, matching_sub_query, compressed_file, compressed_msg) == false) {
            break;
        }

        // Decompress match
        bool decompress_successful = archive.decompress_message(compressed_file, compressed_msg, decompressed_msg);
        if (!decompress_successful) {
            break;
        }

        // Perform wildcard match if required
        // Check if:
        // - Sub-query requires wildcard match, or
        // - no subqueries exist and the search string is not a match-all
        if ((query.contains_sub_queries() && matching_sub_query->wildcard_match_required()) ||
            (query.contains_sub_queries() == false && query.search_string_matches_all() == false))
        {
            bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                                 query.get_ignore_case() == false);
            if (!matched) {
                continue;
            }
        }

        // Print match
        output_func(orig_file_path, compressed_msg, decompressed_msg, output_func_arg);
        ++num_matches;
    }

    return num_matches;
}

bool Grep::search_and_decompress (const Query& query, CLPArchive& archive, CLPFile& compressed_file, Message& compressed_msg, string& decompressed_msg) {
    const string& orig_file_path = compressed_file.get_orig_path();

    bool matched = false;
    while (false == matched) {
        // Find matching message
        const SubQuery* matching_sub_query = nullptr;
        bool message_found = find_matching_message(query, archive, matching_sub_query, compressed_file, compressed_msg);
        if (false == message_found) {
            return false;
        }

        // Decompress match
        bool decompress_successful = archive.decompress_message(compressed_file, compressed_msg, decompressed_msg);
        if (false == decompress_successful) {
            return false;
        }

        // Perform wildcard match if required
        // Check if:
        // - Sub-query requires wildcard match, or
        // - no subqueries exist and the search string is not a match-all
        if ((query.contains_sub_queries() && matching_sub_query->wildcard_match_required()) ||
            (query.contains_sub_queries() == false && query.search_string_matches_all() == false))
        {
            matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                            query.get_ignore_case() == false);
        } else {
            matched = true;
        }
    }

    return true;
}

size_t Grep::search (const Query& query, size_t limit, CLPArchive& archive, CLPFile& compressed_file) {
    size_t num_matches = 0;

    Message compressed_msg;
    string decompressed_msg;
    const string& orig_file_path = compressed_file.get_orig_path();
    while (num_matches < limit) {
        // Find matching message
        const SubQuery* matching_sub_query = nullptr;
        if (find_matching_message(query, archive, matching_sub_query, compressed_file, compressed_msg) == false) {
            break;
        }

        // Perform wildcard match if required
        // Check if:
        // - Sub-query requires wildcard match, or
        // - no subqueries exist and the search string is not a match-all
        if ((query.contains_sub_queries() && matching_sub_query->wildcard_match_required()) ||
            (query.contains_sub_queries() == false && query.search_string_matches_all() == false))
        {
            // Decompress match
            bool decompress_successful = archive.decompress_message(compressed_file, compressed_msg, decompressed_msg);
            if (!decompress_successful) {
                break;
            }

            bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                                 query.get_ignore_case() == false);
            if (!matched) {
                continue;
            }
        }

        ++num_matches;
    }

    return num_matches;
}


// not ready yet
ErrorCode Grep::search_segment_and_send_results_optimized (const std::vector<LogtypeQueries>& queries, const Query& query, size_t limit, GLTArchive& archive, const std::atomic_bool& query_cancelled, int controller_socket_fd) {

    size_t num_matches = 0;
    ErrorCode error_code = ErrorCode_Success;
    // Go through each logtype
    for(const auto& query_for_logtype: queries) {
        // preload the data
        if(query_cancelled) {
            break;
        }
        auto logtype_id = query_for_logtype.m_logtype_id;
        const auto& sub_queries = query_for_logtype.m_queries;
        archive.get_table_manager().load_single_table(logtype_id);

        size_t left_boundary, right_boundary;
        Grep::get_boundaries(sub_queries, left_boundary, right_boundary);

        archive.get_table_manager().load_ts();
        archive.get_table_manager().load_partial_columns(left_boundary, right_boundary);

        auto num_vars = archive.get_logtype_dictionary().get_entry(logtype_id).get_num_vars();
        std::vector<size_t> matched_row_ix;
        std::vector<bool> wildcard_required;
        // Find matching message
        archive.find_message_matching_with_logtype_query_optimized(sub_queries, wildcard_required, query, matched_row_ix);
        // Decompress match
        size_t potential_matches = matched_row_ix.size();
        if(potential_matches != 0 && false == query_cancelled) {
            std::vector<epochtime_t> loaded_ts(potential_matches);
            std::vector<file_id_t> loaded_file_id (potential_matches);
            std::vector<encoded_variable_t> loaded_vars (potential_matches * num_vars);
            archive.get_table_manager().m_single_table.load_remaining_data_into_vec(loaded_ts, loaded_file_id, loaded_vars, matched_row_ix);
            error_code = archive.decompress_messages_and_send_result(logtype_id, loaded_ts, loaded_file_id, loaded_vars, wildcard_required, query, query_cancelled, controller_socket_fd);
        }
        archive.get_table_manager().close_single_table();
        if(error_code != ErrorCode_Success) {
            return error_code;
        }
    }

    return error_code;
}

ErrorCode Grep::search_combined_table_and_send_results (combined_table_id_t table_id, const std::vector<LogtypeQueries>& queries, const Query& query,
                                                       size_t limit, GLTArchive& archive, const std::atomic_bool& query_cancelled, int controller_socket_fd) {

    GLTMessage compressed_msg;
    string decompressed_msg;

    archive.get_table_manager().open_combined_table(table_id);
    for(const auto& iter: queries) {
        logtype_dictionary_id_t logtype_id = iter.m_logtype_id;
        archive.get_table_manager().open_combined_logtype_table(logtype_id);

        const auto& queries_by_logtype = iter.m_queries;

        // Initialize message
        auto num_vars = archive.get_logtype_dictionary().get_entry(logtype_id).get_num_vars();
        compressed_msg.resize_var(num_vars);
        compressed_msg.set_logtype_id(logtype_id);

        size_t left_boundary, right_boundary;
        Grep::get_boundaries(queries_by_logtype, left_boundary, right_boundary);

        bool required_wild_card;
        while(query_cancelled == false) {
            // Find matching message
            bool found_matched = archive.find_message_matching_with_logtype_query_from_combined(queries_by_logtype, compressed_msg, required_wild_card, query, left_boundary, right_boundary);
            if (found_matched == false) {
                break;
            }
            // Decompress match
            bool decompress_successful = archive.decompress_message_with_fixed_timestamp_pattern(compressed_msg, decompressed_msg);
            if (!decompress_successful) {
                break;
            }

            // Perform wildcard match if required
            // Check if:
            // - Sub-query requires wildcard match, or
            // - no subqueries exist and the search string is not a match-all
            if ((query.contains_sub_queries() && required_wild_card) ||
                (query.contains_sub_queries() == false && query.search_string_matches_all() == false)) {
                bool matched = wildcard_match_unsafe(decompressed_msg, query.get_search_string(),
                                                     query.get_ignore_case() == false);
                if (!matched) {
                    continue;
                }
            }
            std::string orig_file_path = archive.get_file_name(compressed_msg.get_file_id());
            msgpack::type::tuple<std::string, epochtime_t, std::string> src(orig_file_path, compressed_msg.get_ts_in_milli(), decompressed_msg);
            msgpack::sbuffer m;
            msgpack::pack(m, src);
            ErrorCode ret = networking::try_send(controller_socket_fd, m.data(), m.size());
            if(ret != ErrorCode_Success) {
                return ret;
            }
        }
        archive.get_table_manager().m_combined_table.close_logtype_table();
    }
    archive.get_table_manager().close_combined_table();
    return ErrorCode_Success;
}

std::unordered_map<logtype_dictionary_id_t, LogtypeQueries> Grep::get_converted_logtype_query (const Query& query, size_t segment_id) {

    // use a map so that queries are ordered by ascending logtype_id
    std::unordered_map<logtype_dictionary_id_t, LogtypeQueries> converted_logtype_based_queries;
    const auto& relevant_subqueries = query.get_relevant_sub_queries();
    for(const auto& sub_query : relevant_subqueries) {

        // loop through all possible logtypes
        const auto& possible_log_entries = sub_query->get_possible_logtype_entries();
        for(const auto& possible_logtype_entry : possible_log_entries) {

            // create one LogtypeQuery for each logtype
            logtype_dictionary_id_t possible_logtype_id = possible_logtype_entry->get_id();

            // now we will get the boundary of the variables for this specific logtype.
            const std::string& possible_logtype_value = possible_logtype_entry->get_value();
            size_t left_boundary = get_variable_front_boundary_delimiter(sub_query->m_tokens, possible_logtype_value);
            size_t right_boundary = get_variable_back_boundary_delimiter(sub_query->m_tokens, possible_logtype_value);
            size_t left_var_boundary = possible_logtype_entry->get_var_left_index_based_on_left_boundary(left_boundary);
            size_t right_var_boundary = possible_logtype_entry->get_var_right_index_based_on_right_boundary(right_boundary);

            LogtypeQuery query_info(sub_query->get_vars(), sub_query->wildcard_match_required(), left_var_boundary, right_var_boundary);

            // The boundary is a range like [left:right). note it's open on the right side
            const auto& containing_segments = possible_logtype_entry->get_ids_of_segments_containing_entry();
            if(containing_segments.find(segment_id) != containing_segments.end()) {
                if(converted_logtype_based_queries.find(possible_logtype_id) == converted_logtype_based_queries.end()) {
                    converted_logtype_based_queries[possible_logtype_id].m_logtype_id = possible_logtype_id;
                }
                converted_logtype_based_queries[possible_logtype_id].m_queries.push_back(query_info);
            }
        }
    }
    return converted_logtype_based_queries;
}

void Grep::get_boundaries(const std::vector<LogtypeQuery>& sub_queries, size_t& left_boundary, size_t& right_boundary) {
    left_boundary = SIZE_MAX;
    right_boundary = 0;
    // HACK: rethink about the current implementation
//    if(sub_queries.size() > 1) {
//        // we use a simple assumption atm.
//        // if subquery1 has range (a,b) and subquery2 has range (c,d).
//        // then the range will be (min(a,c), max(b,d)), even if c > b.
//        SPDLOG_DEBUG("Maybe this is not optimal");
//    }
    for(auto const& subquery : sub_queries) {
        // we use a simple assumption atm.
        // if subquery1 has range (a,b) and subquery2 has range (c,d).
        // then the range will be (min(a,c), max(b,d)), even if c > b.
        if(left_boundary > subquery.m_l_b) {
            left_boundary = subquery.m_l_b;
        }
        if(right_boundary < subquery.m_r_b) {
            right_boundary = subquery.m_r_b;
        }
    }
}