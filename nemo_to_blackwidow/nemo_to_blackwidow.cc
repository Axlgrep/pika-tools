#include "iostream"

#include "nemo.h"
#include "blackwidow/blackwidow.h"

#include "migrator.h"
#include "classify_thread.h"

std::string nemo_db_path;
std::string blackwidow_db_path;
int32_t thread_num;

std::vector<Migrator*> migrators;
std::vector<ClassifyThread*> classify_threads;

void PrintConf() {
  std::cout << "nemo_db_path : " << nemo_db_path << std::endl;
  std::cout << "blackwidow_db_path : " << blackwidow_db_path << std::endl;
  std::cout << "thread_num : " << thread_num << std::endl;
  std::cout << "====================================" << std::endl;

}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "      ./nemo_to_blackwidow nemo_db_path blackwidow_db_path -n [thread_num]\n";
  std::cout << "      example: ./nemo_to_blackwidow ./nemo_db ./blackwidow_db -n 10\n";
}

int main(int argc, char **argv) {
  if (argc != 5) {
    Usage();
    return -1;
  }

  nemo_db_path = std::string(argv[1]);
  blackwidow_db_path = std::string(argv[2]);
  thread_num = atoi(argv[4]);
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

  for (int32_t idx = 0; idx < thread_num; ++idx) {
    migrators.push_back(new Migrator(blackwidow_db));
  }

  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::kKV_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::kHASH_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::kLIST_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::kSET_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::kZSET_DB));

  delete nemo_db;
  delete blackwidow_db;
  return 0;
}
