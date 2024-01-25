// C libraries
#include <sys/stat.h>

// C++ libraries
#include <iostream>
#include <fstream>
#include <filesystem>

int main (int argc, const char* argv[]) {
    std::string temp_metadata_db_path = std::filesystem::path("/home/haiqixu/temp_metadata/") / "metadata.db";
    std::string metadata_db_path = std::filesystem::path("/home/haiqixu/mount_path/aldkshfasjfhajskda") / "metadata.db";
    std::filesystem::copy(temp_metadata_db_path, metadata_db_path);
}
