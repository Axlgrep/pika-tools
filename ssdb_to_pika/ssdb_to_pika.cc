#include <iostream>
#include <thread>
#include <ctime>

#include "SSDB_client.h"
#include "blackwidow.h"
#include "nemo.h"
const int kBatchLen = 1000;
const int kSplitNum = 500000;
//----------------------------
//blackwidow
//----------------------------
void BlackwidowMigrateKv(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db, int& kv_num,
    const std::string& start = "", const std::string& end = "") {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Kv client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Kv client auth error" << std::endl;
      delete client;
      return;
    }	
  }
  std::vector<std::string> kvs;
  ssdb::Status status_ssdb;
  blackwidow::Status status_blackwidow;
  std::string prev_start = start;
  while (true) {
    kvs.clear();
    status_ssdb = client->scan(prev_start, end, kBatchLen, &kvs);  //The two elements at ret[n] and
                                                                  //ret[n+1] form a key-value pair, n=0,2,4,...

    if (!status_ssdb.ok() && kvs.size() % 2 != 0) {
      std::cout << "kv client scan error" << std::endl;
      break;
    }
    if (kvs.size() == 0) {
      break;
    }
    kv_num += kvs.size() >> 1;
    for (auto iter = kvs.begin(); iter != kvs.end(); iter += 2) {
      const std::vector<std::string>* tmp_resp;
      tmp_resp = client->request("ttl", *iter);
      if (!tmp_resp || tmp_resp->empty() || tmp_resp->front() != "ok"){
        std::cout << "Kv client get ttl error" << std::endl;
        delete client;
        return;
      }
      std::string ttl = *(tmp_resp->begin() + 1);
      if ("-1" == ttl) {
        status_blackwidow = db->Set(*iter, *(iter + 1));
      }
      else {
        int32_t time_to_live = atoi(ttl.c_str());
        status_blackwidow = db->Setex(*iter, *(iter + 1), time_to_live);
      }
      //std::cout << "set " << *iter << " " << *(iter + 1) << std::endl;
      if (!status_blackwidow.ok()) {
        std::cout << "Kv client set error, key: " << *iter << std::endl;
      }
    }
    prev_start = kvs[kvs.size() - 2];
  }
    delete client;
    //std::cout << std::this_thread::get_id() << ", Kv client done" << std::endl;

}

void BlackwidowMigrateHash(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db,
    std::vector<std::string> keys, int& hash_num) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Hash client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Hash client auth error" << std::endl;
      delete client;
      return;
    }
  }
  //std::cout << std::this_thread::get_id() << ", Hash client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> fvs;
  ssdb::Status status_ssdb;
  blackwidow::Status status_blackwidow;
  std::string prev_start_field = "";
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    prev_start_field = "";
    while (true) {
      fvs.clear();
      status_ssdb = client->hscan(*iter, prev_start_field, "", kBatchLen, &fvs);
      if (!status_ssdb.ok() || fvs.size() % 2 != 0) {
        std::cout << "Hash client hscan error" << std::endl;
        delete client;
        return;
      }
      hash_num += fvs.size() >> 1;
      if (fvs.empty()) {
        break;
      }
      for (auto it = fvs.begin(); it != fvs.end(); it += 2) {
        int32_t ret;
        status_blackwidow = db->HSet(*iter, *it, *(it + 1), &ret);
        if (!status_blackwidow.ok()) {
          std::cout << "Hash client hset error, key: " << *iter << std::endl;
          delete client;
          return;
        }	
      }
      prev_start_field = fvs[fvs.size() - 2];
    }
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Hash client done" << std::endl;
}

void BlackwidowMigrateQueue(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db,
    std::vector<std::string> keys, int& list_num) {
    ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Queue client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Queue client auth error" << std::endl;
      delete client;
      return;
    }
  }
  //std::cout << std::this_thread::get_id() << ", Queue client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> fs;
  ssdb::Status status_ssdb;
  blackwidow::Status status_blackwidow;
  int64_t start = 0;
  uint64_t len = 0;
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    start = 0;
    while (true) {
      fs.clear();
      status_ssdb = client->qrange(*iter, start, kBatchLen, &fs);
      if (!status_ssdb.ok()) {
        std::cout << "Queue client range error" << std::endl;
        delete client;
        return;
      }
      if (fs.empty()) {
        break;
      } 
      status_blackwidow = db->RPush(*iter, fs, &len);
      if (!status_blackwidow.ok()) {
        std::cout << "Queue client rpush error, key: " << *iter << std::endl;
        delete client;
        return;
      }
      list_num += fs.size();
      start += fs.size();
    }
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Queue client done" << std::endl;
}

void BlackwidowMigrateZset(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db,
    std::vector<std::string> keys, int& zset_num) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Zset client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Zset client auth error" << std::endl;
      delete client;
      return;
    }
  }
  //std::cout << std::this_thread::get_id() << ", Zset client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> sms;
  ssdb::Status status_ssdb;
  blackwidow::Status status_blackwidow;
  std::string prev_start_member = "";
  int32_t zadd_res;
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    prev_start_member = "";
    while (true) {
      sms.clear();
      status_ssdb = client->zscan(*iter, prev_start_member, NULL, NULL, kBatchLen, &sms);
      if (!status_ssdb.ok() || sms.size() % 2 != 0) {
        std::cout << "Zset client zscan error" << std::endl;
        delete client;
        return;
      }
      if (sms.empty()) {
        break;
      }
      std::vector<blackwidow::ScoreMember> stm(sms.size() / 2);
      zset_num += stm.size();
      for (int index = 0; index < stm.size(); index++) {
        stm[index].member = sms[index * 2];
        stm[index].score = std::stod(sms[index * 2 + 1]);
      }
      status_blackwidow = db->ZAdd(*iter, stm, &zadd_res);
      if(!status_blackwidow.ok()) {
        std::cout << "Zadd client zadd error, key: " << *iter << std::endl;
        delete client;
        return;
      }
      prev_start_member = sms[sms.size() - 2];
    }
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Zset client done" << std::endl;
}

void BlackwidowDoKv(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Kv center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Kv center client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::vector<std::string> keys;
  std::thread *threads[1000];
  int num_of_kv[1000];
  for (int i = 0; i < 1000; i++) {
    num_of_kv[i] = 0;
  }
  int thread_num = 0;
  std::string prev_start = "";
  while (true) {
    keys.clear();
    end = "";
    status_ssdb = client->keys(start, end, kSplitNum, &keys);   
    if (!status_ssdb.ok()) {
      std::cout << "Kv center client keys error" << std::endl;
      delete client;
      break;
    }
    if (keys.empty()) {
      std::cout << "Kv center client dispatch keys done " << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(BlackwidowMigrateKv, ip, port,
                            password, db, std::ref(num_of_kv[thread_num]), prev_start, keys.back());
    thread_num++;
    start = prev_start = keys.back();
  }

  for (int i = 0; i < thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  int total_num = 0;
  for (int i = 0; i < thread_num; i++){
    total_num += num_of_kv[i];
  }
  std::cout << "==============Kv migrate done=================" << std::endl;
  std::cout << "Moving kv threads num: " << thread_num << std::endl;
  std::cout << "Total kv nums: " << total_num << std::endl;
  std::cout << "==============================================" << std::endl;
}

void BlackwidowDoHash(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Hash center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Hash center client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int thread_num = 0;
  int num_of_hash[1000];
  for (int i = 0; i < thread_num; i++) {
    num_of_hash[i] = 0;
  } 
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("hlist", start,
                   end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Hash center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Hash center client dispatch keys done " << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(BlackwidowMigrateHash, ip, port, 
                          password, db, keys, std::ref(num_of_hash[thread_num]));
    thread_num++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  int total_num = 0;
  for (int i = 0; i < thread_num; i++) {
    total_num += num_of_hash[i]; 
  }
  std::cout << "=============Hash migrate done================" << std::endl;
  std::cout << "Moving hash threads nums: " << thread_num << std::endl;
  std:: cout << "Toal hash pair nums: " << total_num << std::endl;
  std::cout << "==============================================" << std::endl;
}

void BlackwidowDoZset(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Zset center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Zset center client auth error" << std::endl;
      delete client;
      return;
    }
  }

  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int num_of_zset[1000];
  for (int i = 0; i < 1000; i++) {
    num_of_zset[i] = 0;
  }
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("zlist", start,
                    end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Zset center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Zset center client dispatch keys done " << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(BlackwidowMigrateZset, ip, port, 
                             password, db, keys, std::ref(num_of_zset[thread_num]));
    thread_num++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  int total_num = 0;
  for (int i = 0; i < thread_num; i++) {
    total_num += num_of_zset[i];
  }
  std::cout << "==============Zset migrate done===============" << std::endl;
  std::cout << "Moving zset thread nums: " << thread_num << std::endl;
  std::cout << "Total zset member nums: " << total_num << std::endl;
  std::cout << "==============================================" << std::endl;
}

void BlackwidowDoQueue(const std::string& ip, const int port,
    const std::string& password, blackwidow::BlackWidow* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Queue center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Queue center client auth error" << std::endl;
      delete client;
      return;
    }
  }

  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int num_of_list[1000];
  for (int i = 0; i < 1000; i++) {
    num_of_list[i] = 0;
  }
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("qlist", start,
                    end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Queue center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());
    if (keys.empty()) {
      std::cout << "Queue center client dispatch keys done " << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(BlackwidowMigrateQueue, ip, port,
                         password, db, keys, std::ref(num_of_list[thread_num]));
    thread_num++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  
  int total_num = 0;
  for (int i = 0; i < thread_num; i++) {
    total_num += num_of_list[i];
  }
  std::cout << "=============Queue migrate done===============" << std::endl;
  std::cout << "Moving list threads num: " << thread_num << std::endl;
  std::cout << "List members num: " << total_num << std::endl;
  std::cout << "==============================================" << std::endl;
}


//-------------------------------
// nemo
//-------------------------------


void NemoMigrateKv(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db, int& kv_num,
    const std::string& start = "", const std::string& end = "") {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Kv client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Kv client auth error" << std::endl;
      delete client;
      return;
    }
  }
  //std::cout << std::this_thread::get_id() << ", Kv client start to migrate, from " << start << " to " << end << std::endl;

  std::vector<std::string> kvs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start = start;
  while (true) {
    kvs.clear();
    status_ssdb = client->scan(prev_start, end, kBatchLen, &kvs);

    if(!status_ssdb.ok() && kvs.size() % 2 != 0) {
      std::cout << "kv client scan error" << std::endl;
      break;
    }
    kv_num += kvs.size() >> 1;
    if (kvs.size() == 0) {
      break;
    }
    for (auto iter = kvs.begin(); iter != kvs.end(); iter += 2) {
      //std::cout << "set " << *iter << " " << *(iter+1) << std::endl;
      const std::vector<std::string>* tmp_resp;
      tmp_resp = client->request("ttl", *iter);
      if(!tmp_resp || tmp_resp->empty() || tmp_resp->front() != "ok"){
        std::cout << "Kv client get ttl error" << std::endl;
        delete client;
        return;
      }
      std::string ttl = *(tmp_resp->begin() + 1);
      if ("-1" == ttl) {
      	//std::cout << "set " << *iter << " " << *(iter+1) << std::endl;
      	status_nemo = db->Set(*iter, *(iter+1));
      }
      else {
        int32_t time_to_live = atoi(ttl.c_str());
        //std::cout << "set " << *iter << " " << *(iter + 1) <<"  time to live  " << ttl << std::endl;
        status_nemo = db->Set(*iter, *(iter + 1), time_to_live);
      }
      if (!status_nemo.ok()) {
        std::cout << "Kv client set error, key: " << *iter << std::endl;
      }
    }
    prev_start = kvs[kvs.size() - 2];
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Kv client done" << std::endl;

}

void NemoMigrateHash(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db,
    std::vector<std::string> keys, int& hash_num) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Hash client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Hash client auth error" << std::endl;
      delete client;
      return;
    }
  }
  //std::cout << std::this_thread::get_id() << ", Hash client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> fvs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start_field = "";
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    prev_start_field = "";
    while (true) {
      fvs.clear();
      status_ssdb = client->hscan(*iter, prev_start_field, "", kBatchLen, &fvs);
      if (!status_ssdb.ok() || fvs.size() % 2 != 0) {
        std::cout << "Hash client hscan error" << std::endl;
        delete client;
        return;
      }
      if (fvs.empty()) {
        break;
      }
      hash_num += fvs.size() >> 1; 		
      for (auto it = fvs.begin(); it != fvs.end(); it += 2) {
	//std::cout << "hset " << *iter << " " << *it << " " << *(it + 1) << std::endl;
        status_nemo = db->HSet(*iter, *it, *(it + 1));
        if (!status_nemo.ok()) {
          std::cout << "Hash client hset error, key: " << *iter << std::endl;
          delete client;
          return;
        }
      }
      prev_start_field = fvs[fvs.size() - 2];
    }
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Hash client done" << std::endl;
}

void NemoMigrateQueue(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db,
    std::vector<std::string> keys, int& list_num) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Queue client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Queue client auth error" << std::endl;
      delete client;
      return;
    }
  }
  //std::cout << std::this_thread::get_id() << ", Queue client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> fs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  int64_t start = 0;
  int64_t len = 0;
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    start = 0;
    while (true) {
      fs.clear();
      status_ssdb = client->qrange(*iter, start, kBatchLen, &fs);
      if (!status_ssdb.ok()) {
        std::cout << "Queue client range error" << std::endl;
        delete client;
        return;
      }
      list_num += fs.size() >> 1;
      if (fs.empty()) {
        break;
      }
      for (auto it = fs.begin(); it != fs.end(); it++) {
	//std::cout << "rpush " << *iter << " " << *it << " " << std::endl;
        status_nemo = db->RPush(*iter, *it, &len);
        if (!status_nemo.ok()) {
          std::cout << "Queue client rpush error, key: " << *iter << std::endl;
          delete client;
          return;
        }
      }
      start += fs.size();
    }
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Queue client done" << std::endl;
}

void NemoMigrateZset(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db,
    std::vector<std::string> keys, int& num_zset) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Zset client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Zset client auth error" << std::endl;
      delete client;
      return;
    }
  }
  //std::cout << std::this_thread::get_id() << ", Zset client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> sms;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start_member = "";
  int64_t zadd_res;
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    prev_start_member = "";
    while (true) {
      sms.clear();
      status_ssdb = client->zscan(*iter, prev_start_member, NULL, NULL, kBatchLen, &sms);
      if (!status_ssdb.ok() || sms.size() % 2 != 0) {
        std::cout << "Zset client zscan error" << std::endl;
        delete client;
        return;
      }
      if (sms.empty()) {
        break;
      }
      num_zset += sms.size() >> 1;
      for (auto it = sms.begin(); it != sms.end(); it += 2) {
        //std::cout << "zadd " << *iter << " " << *it << " " << *(it+1) << std::endl;
        status_nemo = db->ZAdd(*iter, stod(*(it + 1)), *it, &zadd_res);
        if (!status_nemo.ok()) {
          std::cout << "Zadd client zadd error, key: " << *iter << std::endl;
          delete client;
          return;
        }
      }
      prev_start_member = sms[sms.size() - 2];
    }
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Zset client done" << std::endl;
}

void NemoDoKv(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Kv center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Kv center client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::vector<std::string> keys;
  std::thread *threads[1000];
  int num_of_kv[1000];
  for (int i = 0; i < 1000; i++) {
    num_of_kv[i] = 0;
  }
  int thread_num = 0;
  std::string prev_start = "";
  while (true) {
    keys.clear();
    end = "";
    status_ssdb = client->keys(start, end, kSplitNum, &keys);
    if (!status_ssdb.ok()) {
      std::cout << "Kv center client keys error" << std::endl;
      delete client;
      break;
    }
    if (keys.empty()) {
      std::cout << "Kv center client dispatch keys done"<< std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(NemoMigrateKv, ip, port, 
      password, db, std::ref(num_of_kv[thread_num]), prev_start, keys.back());
    thread_num++;
    start = prev_start = keys.back();
  }

  for (int i = 0; i < thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  int total_num = 0;
  for (int i = 0; i < thread_num; i++) {
    total_num += num_of_kv[i];
  }
  std::cout << "==============Kv migrate done=================" << std::endl;
  std::cout << "moving kv threads num :" << thread_num << std::endl;
  std::cout << "total kv num:" << total_num << std::endl;
  std::cout << "==============================================" << std::endl; 
}

void NemoDoHash(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Hash center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Hash center client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int num_of_hash[1000];
  for (int i = 0; i < 1000; i++) {
    num_of_hash[i] = 0;
  }
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("hlist", start,
             end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Hash center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Hash center client dispatch keys done" << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(NemoMigrateHash, ip, port,
    password, db, keys, std::ref(num_of_hash[thread_num]));
    thread_num ++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
     threads[i]->join();
     delete threads[i];
  }
  int total_num = 0;
  for (int i = 0; i < thread_num; i++) {
    total_num += num_of_hash[i];
  }
  std::cout << "===============Hash migrate done==============" << std::endl;
  std::cout << "Moving hash thread num " << thread_num << std::endl;
  std::cout << "total hash pairs " << total_num << std::endl;
  std::cout << "==============================================" << std::endl;
}

void NemoDoZset(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Zset center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Zset center client auth error" << std::endl;
      delete client;
      return;
    }
  }

  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int num_of_zset[1000];
  for (int i = 0; i < 1000; i++) {
    num_of_zset[i] = 0;
  }
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("zlist", start, end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Zset center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Zset center client dispatch keys done " << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(NemoMigrateZset, ip, port, 
                            password, db, keys, std::ref(num_of_zset[thread_num]));
    thread_num++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  int total_num = 0;
  for(int i = 0; i < thread_num; i++) {
    total_num += num_of_zset[i];  
  }  

  std::cout << "===============Zset migrate done==============" << std::endl;
  std::cout << "Moving zset thread_num: " << thread_num << std::endl;
  std::cout << "total zset member: " << total_num << std::endl;
  std::cout << "==============================================" << std::endl; 
}

void NemoDoQueue(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Queue center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Queue center client auth error" << std::endl;
      delete client;
      return;
    }
  }

  std::string start = "";
  std::string end = "";

  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int num_of_list[1000];
  for (int i = 0; i < 1000; i++) {
    num_of_list[i] = 0;
  }
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("qlist", start, end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Queue center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Queue center client dispatch keys done " << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(NemoMigrateQueue, ip, port,
				     password, db, keys, std::ref(num_of_list[thread_num]));
    thread_num ++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }
  int total_num = 0;
  for (int i = 0; i < thread_num; i++) {
    total_num += num_of_list[thread_num];
  }
  std::cout << "=============Queue migrate done===============" << std::endl;
  std::cout << "Moving list threads num: " << thread_num << std::endl;
  std::cout << "Total list members num: " << total_num << std::endl; 
  std::cout << "==============================================" << std::endl; 
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tSsdb_to_pika read data from SSDB and send to Nemo or Blackwidow" << std::endl;
  std::cout << "\t--host            ssdb_server_ip" << std::endl;
  std::cout << "\t--port            ssdb_server_port" << std::endl;
  std::cout << "\t--path            pika db's path"<< std::endl;
  std::cout << "\t--form            nemo or blackwidow based on your pika version" << std::endl;
  std::cout << "\t--[password]      ssdb_server_password if it has password" << std::endl;
  std::cout << "\texample: ./ssdb_to_pika 127.0.0.1 8888 ./db blackwidow" << std::endl;
}
void printInfo(int sign, const std::time_t& now, std::string& ip, int port, std::string& path) {
  if (sign == 0){  
    std::cout << "=============ssdb to nemo=====================" << std::endl;
  }
  else {
    std::cout << "=============ssdb to balckwidow===============" << std::endl;    
  }
  std::cout <<   "ssdb_server ip:      " << ip << std::endl;
  std::cout <<   "ssdb_server port:    " << port << std::endl;
  std::cout <<   "db path:             " << path << std::endl;
  std::cout <<   "Start Up time:       " << ctime(&now) << std::endl;		
  std::cout <<   "==============================================" << std::endl;
}
int main(int argc, char** argv) {
  if (argc != 5 && argc != 6) {
    Usage();
    return -1;
  }
  std::string ip = argv[1];
  int port = atoi(argv[2]);
  std::string path = argv[3];
  std::string mode = argv[4];
  std::string password = "";
  if (argc == 6) {
    password = argv[5];
  }
  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  if (mode == "nemo") {
    nemo::Options option;
    option.write_buffer_size = 268435456; //256M
    option.target_file_size_base = 20971520; //20M
    option.max_background_flushes = 4;
    option.max_background_compactions = 4;
    nemo::Nemo* db = new nemo::Nemo(path, option);
    printInfo(0, now, ip, port, path);
    std::thread thread_kv = std::thread(NemoDoKv, ip, port, password, db);
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
    std::thread thread_hash = std::thread(NemoDoHash, ip, port, password, db); 
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread thread_zset = std::thread(NemoDoZset, ip, port, password, db);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread thread_queue = std::thread(NemoDoQueue, ip, port, password, db);
    thread_kv.join();
    thread_hash.join();
    thread_zset.join();
    thread_queue.join();
    delete db;
  }
  else if (mode == "blackwidow") {
    blackwidow::BlackwidowOptions option;
    blackwidow::BlackWidow* db = new blackwidow::BlackWidow();
    option.options.create_if_missing = true;
    db->Open(option, path);
    printInfo(1, now, ip, port, path);
    std::thread thread_kv = std::thread(BlackwidowDoKv, ip, port, password, db);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread thread_hash = std::thread(BlackwidowDoHash, ip, port, password, db);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread thread_zset = std::thread(BlackwidowDoZset, ip, port, password, db);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread thread_queue = std::thread(BlackwidowDoQueue, ip, port, password, db);
    thread_kv.join();
    thread_hash.join();
    thread_zset.join();
    thread_queue.join();

    delete db;
  }
  else {
    Usage();
    return -1;
  }
  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout <<   "====================END=======================" << std::endl;
  std::cout << "Finish Time : " << asctime(localtime(&now));  
  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
  std::cout << "Total Time Cost : "
            << hours << " hours "
            << minutes % 60 << " minutes "
            << seconds % 60 << " seconds "
            << std::endl;
  return 0;
}
