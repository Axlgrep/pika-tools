#include<iostream>
#include<cstring>
#include<memory>

#include "slave.h"
#include "hiredis.h"

void Usage() {
  std::cout << "Usage:" << std::endl;
  std::cout << "\tssdb_to_pika read data from ssdb sever and send it to pika sever" << std::endl;
  std::cout << "\tssdb_ip	ssdb_server's ip"   << std::endl;
  std::cout << "\tssdb_port	ssdb_server's port" << std::endl;
  std::cout << "\tpika_ip	pika_server's ip"   << std::endl;
  std::cout << "\tpika_port	pika_sever's port"  << std::endl;
  std::cout << "\t[ssdb_auth]   ssdb_server's auth" << std::endl;
  std::cout << "\t[pika_auth]   pika_server's auth" << std::endl;
  std::cout << "\texample: ./ssdb_to_pika localhost 8888 localhost 9925" << std::endl;
}

int main(int argc, char** argv) {
  if (argc < 5) {
    Usage();
    return 1;
  }
  const char* ssdb_auth = nullptr; 
  const char* pika_auth = nullptr;
  if (argc >= 6) { 
    ssdb_auth = argv[5];
  }
  if (argc >= 7) {
    pika_auth = argv[6];
  }
  std::unique_ptr<Slave> slave_ptr(new Slave(argv[1], atoi(argv[2]), argv[3], 
                              atoi(argv[4]), ssdb_auth, pika_auth)); 
  slave_ptr->start();
  pthread_t tid = slave_ptr->getTid();
  pthread_join(tid, NULL);
  if (tid == 0) {
    std::cout << "some error with connection" << std::endl;
    return 0;
  }
  return 0;
}
