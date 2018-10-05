// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_UTILS_TOOLKITS_H__
#define __DATABASE_UTILS_TOOLKITS_H__

#include <boost/filesystem.hpp>
#include <boost/date_time.hpp>
#include <string>
#include <cstdint>

template<typename T1, typename T2>
struct BinaryKeyHash : public std::unary_function<std::tuple<T1, T2>,
    std::size_t> {
  std::size_t operator()(const std::tuple<T1, T2>& k) const {
    return std::get<0>(k) ^ std::get<1>(k);
  }
};

template<typename T1, typename T2>
struct BinaryKeyEqual : public std::binary_function<std::tuple<T1, T2>,
    std::tuple<T1, T2>, bool> {
  bool operator()(const std::tuple<T1, T2>& v0,
                  const std::tuple<T1, T2>& v1) const {
    return (std::get<0>(v0) == std::get < 0 > (v1) &&std::get<1>(v0)
        == std::get<1>(v1));
  }
};

template<typename T1, typename T2, typename T3>
struct TernaryKeyHash : public std::unary_function<std::tuple<T1, T2, T3>,
    std::size_t> {
  std::size_t operator()(const std::tuple<T1, T2, T3>& k) const {
    return std::get<0>(k) ^ std::get<1>(k) ^ std::get<2>(k);
  }
};

template<typename T1, typename T2, typename T3>
struct TernaryKeyEqual : public std::binary_function<std::tuple<T1, T2, T3>,
    std::tuple<T1, T2, T3>, bool> {
  bool operator()(const std::tuple<T1, T2, T3>& v0,
                  const std::tuple<T1, T2, T3>& v1) const {
    return (std::get<0>(v0) == std::get < 0 > (v1) &&std::get<1>(v0)
        == std::get < 1 > (v1) &&std::get<2>(v0) == std::get<2>(v1));
  }
};

// we assume that str is at least 4 bytes (32 bits) long.
static uint32_t FastHash(const char *str) {
  return *(uint32_t*) (str);
}

static uint32_t FastHash(const char *str1, const char *str2) {
  return *(uint32_t*) (str1) + *(uint32_t*) (str2);
}

static uint32_t FastHash(const char *str1, const char *str2, const char *str3) {
  return *(uint32_t*) (str1) + *(uint32_t*) (str2) + *(uint32_t*) (str3);
}

static uint32_t FastHash(const char *str1, const char *str2, const char *str3,
                         const char *str4) {
  return *(uint32_t*) (str1) + *(uint32_t*) (str2) + *(uint32_t*) (str3)
      + *(uint32_t*) (str4);
}

static std::string Int2Str(const int &param) {
  char tmp[20];
#ifdef _WIN32
  sprintf_s(tmp, "%d", param);
#elif __linux__
  sprintf(tmp, "%d", param);
#endif
  return tmp;
}

static std::string & Int2Str(const int &param, std::string &str) {
  char tmp[20];
#ifdef _WIN32
  sprintf_s(tmp, "%d", param);
#elif __linux__
  sprintf(tmp, "%d", param);
#endif
  str = std::string(tmp);
  return str;
}

static std::string PrintCurrentTime() {
  boost::posix_time::ptime c_time =
      boost::posix_time::second_clock::local_time();
  return boost::posix_time::to_iso_string(c_time);
}

static std::string Long2Str(const int64_t &param) {
  char tmp[20];
#ifdef _WIN32
  sprintf_s(tmp, "%ld", param);
#elif __linux__
  sprintf(tmp, "%ld", param);
#endif
  return tmp;
}

static std::string & Long2Str(const int64_t &param, std::string &str) {
  char tmp[20];
#ifdef _WIN32
  sprintf_s(tmp, "%ld", param);
#elif __linux__
  sprintf(tmp, "%ld", param);
#endif
  str = std::string(tmp);
  return str;
}

static int Str2Int(const std::string &param) {
  int res = atoi(param.c_str());
  return res;
}

static int & Str2Int(const std::string &param, int &res) {
  res = atoi(param.c_str());
  return res;
}

static std::string GetConfigFilename(std::string &name) {
  boost::filesystem::create_directories("config");
#ifdef _WIN32
  return "config\\" + name + ".config";
#elif __linux__
  return "config/" + name + "boot.config";
#endif
}

static std::string GetLogFilename(const std::string &name) {
  return name + ".log";
}

static std::string GetLogFilename(const std::string &folder,
                                  const std::string &name) {
  boost::filesystem::create_directories(folder);
#ifdef _WIN32
  return folder + "\\" + name + ".log";
#elif __linux__
  return folder + "/" + name + ".log";
#endif
}

static std::string GetLogFilenameTs(const std::string &prefix) {
  boost::posix_time::ptime c_time =
      boost::posix_time::second_clock::local_time();
  return prefix + "_" + boost::posix_time::to_iso_string(c_time) + ".log";
}

static std::string GetLogFilenameTs(const std::string &folder,
                                    const std::string &prefix) {
  boost::filesystem::create_directories(folder);
  boost::posix_time::ptime c_time =
      boost::posix_time::second_clock::local_time();
#ifdef _WIN32
  return folder + "\\" + prefix + "_" + boost::posix_time::to_iso_string(c_time) + ".log";
#elif __linux__
  return folder + "/" + prefix + "_" + boost::posix_time::to_iso_string(c_time)
      + ".log";
#endif
}

static std::string GetVersionFilename(const std::string &folder,
                                      const int &operator_id,
                                      const int &stream_id,
                                      const int &version) {
  boost::filesystem::create_directories(folder);
#ifdef _WIN32
  return "buffer\\" + Int2Str(operator_id) + "_" + Int2Str(stream_id) + "_" + Int2Str(version) + ".log";
#elif __linux__
  return "buffer/" + Int2Str(operator_id) + "_" + Int2Str(stream_id) + "_"
      + Int2Str(version) + ".log";
#endif
}

static std::string GetVersionFilename(const std::string &folder,
                                      const int &operator_id,
                                      const int &stream_id,
                                      const int &bucket_id,
                                      const int &version) {
  boost::filesystem::create_directories(folder);
#ifdef _WIN32
  return "buffer\\" + Int2Str(operator_id) + "_" + Int2Str(stream_id) + "_" + Int2Str(bucket_id) + "_" + Int2Str(version) + ".log";
#elif __linux__
  return "buffer/" + Int2Str(operator_id) + "_" + Int2Str(stream_id) + "_"
      + Int2Str(bucket_id) + "_" + Int2Str(version) + ".log";
#endif	
}

static std::string GetVersionFilename(const std::string &folder,
                                      const int &operator_id,
                                      const int &version) {
  boost::filesystem::create_directories(folder);
#ifdef _WIN32
  return "state\\" + Int2Str(operator_id) + "_" + Int2Str(version) + ".log";
#elif __linux__
  return "state/" + Int2Str(operator_id) + "_" + Int2Str(version) + ".log";
#endif	
}

#endif
