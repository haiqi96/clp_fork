#include "AwsAuthenticationSigner.hpp"

#include <chrono>
#include <regex>
#include <span>
#include <vector>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include "../ErrorCode.hpp"
#include "../hash_utils.hpp"
#include "../type_utils.hpp"
#include "constants.hpp"

using clp::size_checked_pointer_cast;
using std::span;
using std::string;
using std::string_view;
using std::vector;

namespace clp::aws {
namespace {
/**
 * Gets the timestamp string specified by AWS Signature Version 4 format
 * @param timestamp
 * @return The timestamp string
 */
[[nodiscard]] auto get_timestamp_string(std::chrono::system_clock::time_point const& timestamp
) -> string {
    return fmt::format("{:%Y%m%dT%H%M%SZ}", timestamp);
}

/**
 * Gets the date string specified by AWS Signature Version 4 format
 * @param timestamp
 * @return The date string
 */
[[nodiscard]] auto get_date_string(std::chrono::system_clock::time_point const& timestamp
) -> string {
    return fmt::format("{:%Y%m%d}", timestamp);
}

/**
 * Converts an HttpMethod to a string
 * @param method HTTP method
 * @return The converted string
 */
[[nodiscard]] auto get_method_string(AwsAuthenticationSigner::HttpMethod method) -> string {
    switch (method) {
        case AwsAuthenticationSigner::HttpMethod::GET:
            return "GET";
        case AwsAuthenticationSigner::HttpMethod::PUT:
            return "PUT";
        case AwsAuthenticationSigner::HttpMethod::POST:
            return "POST";
        case AwsAuthenticationSigner::HttpMethod::DELETE:
            return "DELETE";
        default:
            throw std::runtime_error("Invalid HTTP method");
    }
}

/**
 * Gets the string to sign specified by AWS Signature Version 4 format.
 * @param scope
 * @param timestamp_string
 * @param canonical_request
 * @param string_to_sign Returns the string to sign
 * @return ErrorCode_Success on success
 * @return Same as `get_sha256_hash` on failure.
 */
[[nodiscard]] auto get_string_to_sign(
        string_view scope,
        string_view timestamp_string,
        string_view canonical_request,
        string& string_to_sign
) -> ErrorCode {
    vector<unsigned char> signed_canonical_request;
    if (auto error_code = get_sha256_hash(
                {size_checked_pointer_cast<unsigned char const>(canonical_request.data()),
                 canonical_request.size()},
                signed_canonical_request
        );
        ErrorCode_Success != error_code)
    {
        return error_code;
    }
    auto const signed_canonical_request_str = convert_to_hex_string(
            {signed_canonical_request.data(), signed_canonical_request.size()}
    );
    string_to_sign = fmt::format(
            "{}\n{}\n{}\n{}",
            cAws4HmacSha256,
            timestamp_string,
            scope,
            signed_canonical_request_str
    );
    return ErrorCode_Success;
}

/**
 * Encodes the Uri as specified by AWS Signature Version 4's UriEncode()
 * @param Uri
 * @param encode_slash
 * @return The encoded Uri
 */
[[nodiscard]] auto encode_uri(string_view uri, bool encode_slash) -> string {
    string encoded_uri;

    for (auto const c : uri) {
        if ((std::isalnum(c) != 0) || c == '-' || c == '_' || c == '.' || c == '~') {
            encoded_uri += c;
        } else if (c == '/' && false == encode_slash) {
            encoded_uri += c;
        } else {
            encoded_uri += fmt::format("%{:02X}", c);
        }
    }

    return encoded_uri;
}

/**
 * Gets the scope specified by AWS Signature Version 4 format
 * @param date_string
 * @param region
 * @return The scope as a string
 */
[[nodiscard]] auto get_scope(string_view date_string, string_view region) -> string {
    return fmt::format("{}/{}/{}/{}", date_string, region, cS3Service, cAws4Request);
}

/**
 * Gets the canonical request string
 * @param method HTTP method
 * @param url S3 Url
 * @param query_string Query string
 * @return Canonical request
 */
[[nodiscard]] auto get_canonical_request(S3Url const& url, string_view query_string) -> string {
    return fmt::format(
            "{}\n{}\n{}\n{}:{}\n\n{}\n{}",
            get_method_string(AwsAuthenticationSigner::HttpMethod::GET),
            encode_uri(url.get_path(), false),
            query_string,
            cDefaultSignedHeaders,
            url.get_host(),
            cDefaultSignedHeaders,
            cUnsignedPayload
    );
}
}  // namespace

S3Url::S3Url(string const& url) {
    // Virtual-hosted-style HTTP URL format
    std::regex const host_style_url_regex(
            R"(https://([a-z0-9.-]+)\.s3(\.([a-z0-9-]+))?\.amazonaws\.com(/[^?]+).*)"
    );
    // Path-style HTTP URL format
    std::regex const path_style_url_regex(
            R"(https://s3(\.([a-z0-9-]+))?\.amazonaws\.com/([a-z0-9.-]+)(/[^?]+).*)"
    );

    std::smatch match;
    if (std::regex_match(url, match, host_style_url_regex)) {
        m_bucket = match[1].str();
        m_region = match[3].str();
        m_path = match[4].str();
    } else if (std::regex_match(url, match, path_style_url_regex)) {
        m_region = match[2].str();
        m_bucket = match[3].str();
        m_path = match[4].str();
    } else {
        throw OperationFailed(
                ErrorCode_BadParam,
                __FILENAME__,
                __LINE__,
                "Invalid S3 HTTP URL format: " + url
        );
    }

    if (m_region.empty()) {
        m_region = cDefaultRegion;
    }
    m_host = fmt::format("{}.s3.{}.amazonaws.com", m_bucket, m_region);
}

auto AwsAuthenticationSigner::generate_presigned_url(
        S3Url const& s3_url,
        string& presigned_url
) const -> ErrorCode {
    auto const s3_region = s3_url.get_region();

    auto const now = std::chrono::system_clock::now();
    auto const date_string = get_date_string(now);
    auto const timestamp_string = get_timestamp_string(now);

    auto const scope = get_scope(date_string, s3_region);
    auto const canonical_query_string = generate_canonical_query_string(scope, timestamp_string);

    auto const canonical_request = get_canonical_request(s3_url, canonical_query_string);

    string string_to_sign{};
    if (auto error_code
        = get_string_to_sign(scope, timestamp_string, canonical_request, string_to_sign);
        ErrorCode_Success != error_code)
    {
        return error_code;
    }

    vector<unsigned char> signature{};
    if (auto error_code = get_signature(s3_region, date_string, string_to_sign, signature);
        ErrorCode_Success != error_code)
    {
        return error_code;
    }
    auto const signature_str = convert_to_hex_string({signature.data(), signature.size()});

    presigned_url = fmt::format(
            "https://{}{}?{}&{}={}",
            s3_url.get_host(),
            s3_url.get_path(),
            canonical_query_string,
            cXAmzSignature,
            signature_str
    );
    return ErrorCode_Success;
}

auto AwsAuthenticationSigner::get_signature(
        string_view region,
        string_view date_string,
        string_view string_to_sign,
        vector<unsigned char>& signature
) const -> ErrorCode {
    vector<unsigned char> signing_key{};
    if (auto error_code = get_signing_key(region, date_string, signing_key);
        ErrorCode_Success != error_code)
    {
        return error_code;
    }

    if (auto error_code = get_hmac_sha256_hash(
                {size_checked_pointer_cast<unsigned char const>(string_to_sign.data()),
                 string_to_sign.size()},
                {size_checked_pointer_cast<unsigned char const>(signing_key.data()),
                 signing_key.size()},
                signature
        );
        ErrorCode_Success != error_code)
    {
        return error_code;
    }
    return ErrorCode_Success;
}

auto AwsAuthenticationSigner::get_signing_key(
        string_view region,
        string_view date_string,
        vector<unsigned char>& signing_key
) const -> ErrorCode {
    string key{cAws4};
    key += m_secret_access_key;

    vector<unsigned char> date_key{};
    vector<unsigned char> date_region_key{};
    vector<unsigned char> date_region_service_key{};
    if (auto error_code = get_hmac_sha256_hash(
                {size_checked_pointer_cast<unsigned char const>(date_string.data()),
                 date_string.size()},
                {size_checked_pointer_cast<unsigned char const>(key.data()), key.size()},
                date_key
        );
        error_code != ErrorCode_Success)
    {
        return error_code;
    }

    if (auto error_code = get_hmac_sha256_hash(
                {size_checked_pointer_cast<unsigned char const>(region.data()), region.size()},
                {size_checked_pointer_cast<unsigned char const>(date_key.data()), date_key.size()},
                date_region_key
        );
        error_code != ErrorCode_Success)
    {
        return error_code;
    }

    if (auto error_code = get_hmac_sha256_hash(
                {size_checked_pointer_cast<unsigned char const>(cS3Service.data()),
                 cS3Service.size()},
                {size_checked_pointer_cast<unsigned char const>(date_region_key.data()),
                 date_region_key.size()},
                date_region_service_key
        );
        error_code != ErrorCode_Success)
    {
        return error_code;
    }

    if (auto error_code = get_hmac_sha256_hash(
                {size_checked_pointer_cast<unsigned char const>(cAws4Request.data()),
                 cAws4Request.size()},
                {size_checked_pointer_cast<unsigned char const>(date_region_service_key.data()),
                 date_region_service_key.size()},
                signing_key
        );
        error_code != ErrorCode_Success)
    {
        return error_code;
    }

    return ErrorCode_Success;
}

auto AwsAuthenticationSigner::generate_canonical_query_string(
        string_view scope,
        string_view timestamp_string
) const -> string {
    string uri{m_access_key_id + "/"};
    uri += scope;
    return fmt::format(
            "{}={}&{}={}&{}={}&{}={}&{}={}",
            cXAmzAlgorithm,
            cAws4HmacSha256,
            cXAmzCredential,
            encode_uri(uri, true),
            cXAmzDate,
            timestamp_string,
            cXAmzExpires,
            cDefaultExpireTime,
            cXAmzSignedHeaders,
            cDefaultSignedHeaders
    );
}
}  // namespace clp::aws