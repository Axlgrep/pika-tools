#include "iostream"

#include "nemo.h"
#include "blackwidow/blackwidow.h"

std::string nemo_db_path;
std::string blackwidow_db_path;

void PrintConf() {
  std::cout << "nemo_db_path : " << nemo_db_path << std::endl;
  std::cout << "blackwidow_db_path : " << blackwidow_db_path << std::endl;
  std::cout << "====================================" << std::endl;

}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "      ./nemo_to_blackwidow nemo_db_path blackwidow_db_path\n";
  std::cout << "      example: ./pika_to_redis ./nemo_db ./blackwidow_db\n";
}

int main(int argc, char **argv) {
  if (argc != 3) {
    Usage();
    return -1;
  }

  nemo_db_path = std::string(argv[1]);
  blackwidow_db_path = std::string(argv[2]);
  PrintConf();

  // Init nemo db
  nemo::Options nemo_option;
  nemo_option.create_if_missing = false;
  nemo_option.write_buffer_size = 256 * 1024 * 1024;           // 256M
  nemo_option.target_file_size_base = 20 * 1024 * 1024;        // 20M
  nemo::Nemo* nemo_db = new nemo::Nemo(nemo_db_path, nemo_option);
  if (nemo_db != NULL) {
    std::cout << "Open Nemo db success..." << std::endl;
  } else {
    std::cout << "Open Nemo db failed..." << std::endl;
    return -1;
  }

  // Init blackwidow db
  rocksdb::Status status;
  rocksdb::Options blackwidow_option;
  blackwidow_option.create_if_missing = true;
  blackwidow_option.write_buffer_size = 256 * 1024 * 1024;     // 256M
  blackwidow_option.target_file_size_base = 20 * 1024 * 1024;  // 20M
  blackwidow::BlackWidow* blackwidow_db = new blackwidow::BlackWidow();
  if (blackwidow_db != NULL
    && (status = blackwidow_db->Open(blackwidow_option, blackwidow_db_path)).ok()) {
    std::cout << "Open BlackWidow db success..." << std::endl;
  } else {
    std::cout << "Open BlackWidow db failed..." << std::endl;
    return -1;
  }




  delete nemo_db;
  delete blackwidow_db;
  return 0;
}
