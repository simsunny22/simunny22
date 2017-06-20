#include <iostream>
#include <string>
#include <sys/stat.h>

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;


//g++-5 -std=c++17 fileinfo.cpp (for #include <experimental/filesystem>)
int main() {
  std::string filename = "tmpfile";

  struct stat stat_buf;
  int rc = stat(filename.c_str(), &stat_buf);
  if (rc != 0) {
    std::cout << "tmpfile stat error:" << rc << std::endl;
  }

  std::cout << "tmpfile size:" << stat_buf.st_size << std::endl;
  std::cout << "tmpfile st_blksize:" << stat_buf.st_blksize << " st_blocks:" << stat_buf.st_blocks << std::endl;

  //fs::path p{filename.c_str()};
  //p = fs::canonical(p);

  //std::cout << "tmpfile size:" << fs::file_size(p) << std::endl;

  return 0;
}
