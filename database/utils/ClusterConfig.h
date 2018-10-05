#ifndef __DATABASE_UTILS_CLUSTER_CONFIG_H__
#define __DATABASE_UTILS_CLUSTER_CONFIG_H__

#include <string>
#include <fstream>
#include <vector>

namespace Database {
struct ServerInfo {
  ServerInfo(const std::string& addr, const int port)
      : addr_(addr),
        port_no_(port) {
  }
  ServerInfo() {
  }

  std::string addr_;
  int port_no_;
};

class ClusterConfig {
 public:
  ClusterConfig(const std::string& my_host_name, const int port, 
      const std::string& config_filename)
      : my_info_(my_host_name, port), config_filename_(config_filename) {
    this->ReadConfigFile();
  }
  ~ClusterConfig() {
  }

  ServerInfo GetMyHostInfo() const {
    return my_info_;
  }

  ServerInfo GetMasterHostInfo() const {
    return server_info_.at(0);
  }

  size_t GetPartitionNum() const {
    return server_info_.size();
  }

  size_t GetMyPartitionId() const {
    for (size_t i = 0; i < server_info_.size(); ++i) {
      ServerInfo host = server_info_.at(i);
      if (host.addr_ == my_info_.addr_ && host.port_no_ == my_info_.port_no_) {
        return i;
      }
    }
    return server_info_.size() + 1;
  }

  bool IsMaster() const {
    ServerInfo my = GetMyHostInfo();
    ServerInfo master = GetMasterHostInfo();
    return my.addr_ == master.addr_ && my.port_no_ == master.port_no_;
  }

private:
  void ReadConfigFile() {
    std::string name;
    int port;

    std::ifstream readfile(config_filename_);
    assert(readfile.is_open() == true);
    while (!readfile.eof()) {
      name = "";
      port = -1;
      readfile >> name >> port;
      if (name == "" && port < 0)
        continue;
      server_info_.push_back(ServerInfo(name, port));
    }
    readfile.close();

    for (auto& entry : server_info_) {
      std::cout << "server name=" << entry.addr_ << ",port_no="
                << entry.port_no_ << std::endl;
    }
  }
private:
  std::vector<ServerInfo> server_info_;
  ServerInfo my_info_;
  std::string config_filename_;
};
}

#endif
