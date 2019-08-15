#include <iostream>
#include <thread>
#include <ctime>
#include <atomic>

#include "SSDB_client.h"
#include "blackwidow.h"
#include "nemo.h"
#include "thread_pool.h"

enum DBType{
  NEMO,
  BLACKWIDOW
};

const std::string EXPIRE = "\xff\xff\xff\xff\xff|EXPIRE_LIST|KV";
const int kBatchLen = 1000;
const int kSplitNum = 500000;

//----------------------------
//blackwidow
//----------------------------
void BlackwidowMigrateKv(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db, std::atomic<int>& num_kv_key,
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
    num_kv_key += kvs.size() >> 1;
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
}

void BlackwidowMigrateHash(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db,
    std::vector<std::string> keys, std::atomic<int>& num_hash_key) {
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

  std::vector<std::string> fvs;
  ssdb::Status status_ssdb;
  blackwidow::Status status_blackwidow;
  std::string prev_start_field = "";
  num_hash_key += keys.size();
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
}

void BlackwidowMigrateQueue(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db,
    std::vector<std::string> keys, std::atomic<int>& num_queue_key) {
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

  std::vector<std::string> fs;
  ssdb::Status status_ssdb;
  blackwidow::Status status_blackwidow;
  int64_t start = 0;
  uint64_t len = 0;
  num_queue_key += keys.size();
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
      start += fs.size();
    }
  }
  delete client;
  //std::cout << std::this_thread::get_id() << ", Queue client done" << std::endl;
}

void BlackwidowMigrateZset(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db,
    std::vector<std::string> keys, std::atomic<int>& num_zset_key) {
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
  num_zset_key += keys.size();
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    if (EXPIRE == *iter) {
      num_zset_key--;
      continue;
    } 
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
      for (size_t index = 0; index < stm.size(); index++) {
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
}

void BlackwidowDoKv(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db,
    ThreadPool& dispatcher, std::atomic<int>& num_kv_key) {

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
    std::function<void()> task = std::bind(BlackwidowMigrateKv, ip, port,
                        password, db, std::ref(num_kv_key), prev_start, keys.back());
    dispatcher.AddTask(task);
    start = prev_start = keys.back();
  }
}

void BlackwidowDoHash(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db, 
    ThreadPool& dispatcher, std::atomic<int>& num_hash_key) {

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
    std::function<void()> task = std::bind(BlackwidowMigrateHash, ip, port,
                            password, db, keys, std::ref(num_hash_key));
    dispatcher.AddTask(task);
    start = prev_start = resp->back();
  }
}

void BlackwidowDoZset(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db,
    ThreadPool& dispatcher, std::atomic<int>& num_zset_key) {

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
    std::function<void()> task = std::bind(BlackwidowMigrateZset, ip, port,
                          password, db, keys, std::ref(num_zset_key));
    dispatcher.AddTask(task);
    start = prev_start = resp->back();
  }
}

void BlackwidowDoQueue(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<blackwidow::BlackWidow> const db,
    ThreadPool& dispatcher, std::atomic<int>& num_queue_key) {

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
    std::function<void()> task = std::bind(BlackwidowMigrateQueue, ip, port,
                            password, db, keys, std::ref(num_queue_key));
    dispatcher.AddTask(task);
    start = prev_start = resp->back();
  }

}


//-------------------------------
// nemo
//-------------------------------


void NemoMigrateKv(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<nemo::Nemo> const db, std::atomic<int>& num_kv_key,
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
  nemo::Status status_nemo;

  std::string prev_start = start;
  while (true) {
    kvs.clear();
    status_ssdb = client->scan(prev_start, end, kBatchLen, &kvs);

    if(!status_ssdb.ok() && kvs.size() % 2 != 0) {
      std::cout << "kv client scan error" << std::endl;
      break;
    }
    num_kv_key += kvs.size() >> 1;
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
      	status_nemo = db->Set(*iter, *(iter +1));
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
    const std::string& password, std::shared_ptr<nemo::Nemo> const db,
    std::vector<std::string> keys, std::atomic<int>& num_hash_key) {
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
  std::vector<std::string> fvs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start_field = "";
  num_hash_key += keys.size();
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
}

void NemoMigrateQueue(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<nemo::Nemo> const db,
    std::vector<std::string> keys, std::atomic<int>& num_queue_key) {
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

  std::vector<std::string> fs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  int64_t start = 0;
  int64_t len = 0;
  num_queue_key += keys.size();
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
      for (auto it = fs.begin(); it != fs.end(); it++) {
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
}

void NemoMigrateZset(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<nemo::Nemo> const db,
    std::vector<std::string> keys, std::atomic<int>& num_zset_key) {
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

  std::vector<std::string> sms;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start_member = "";
  int64_t zadd_res;
  num_zset_key += keys.size();
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    if (EXPIRE == *iter) {
      num_zset_key--;
      continue;
    }
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
      for (auto it = sms.begin(); it != sms.end(); it += 2) {
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
}

void NemoDoKv(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<nemo::Nemo> const db, 
    ThreadPool& dispatcher, std::atomic<int>& num_kv_key) {

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
    std::function<void()> task = std::bind(NemoMigrateKv, ip,
                          port, password, db, std::ref(num_kv_key), prev_start, keys.back()); 
    dispatcher.AddTask(task);  
    start = prev_start = keys.back();
  }
}

void NemoDoHash(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<nemo::Nemo> const db, 
    ThreadPool& dispatcher, std::atomic<int>& num_hash_key) {

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
    std::function<void()> task = std::bind(NemoMigrateHash, ip, port,
                password, db, keys, std::ref(num_hash_key));
    dispatcher.AddTask(task);
    start = prev_start = resp->back();
  }
}

void NemoDoZset(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<nemo::Nemo> const db, 
    ThreadPool& dispatcher, std::atomic<int>& num_zset_key) {

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
    std::function<void()> task = std::bind(NemoMigrateZset, ip, port,
                password, db, keys, std::ref(num_zset_key));
    dispatcher.AddTask(task);
    start = prev_start = resp->back();
  }
}

void NemoDoQueue(const std::string& ip, const int port,
    const std::string& password, std::shared_ptr<nemo::Nemo> const db,
    ThreadPool& dispatcher, std::atomic<int>& num_queue_key) {

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
    std::function<void()> task = std::bind(NemoMigrateQueue, ip, port,
                 password, db ,keys ,std::ref(num_queue_key));
    dispatcher.AddTask(task);
    start = prev_start = resp->back();
  }

}

void PrintRes(std::atomic<int>& num_kv_key, std::atomic<int>& num_hash_key, 
                       std::atomic<int>& num_zset_key, std::atomic<int>& num_queue_key) {
  std::cout << "===============Kv migrate done================" << std::endl;
  std::cout << "Total Kv members num: " << num_kv_key << std::endl;
  std::cout << "==============================================" << std::endl; 

  std::cout << "=============Hash migrate done================" << std::endl;
  std::cout << "Total Hash members num: " << num_hash_key << std::endl;
  std::cout << "==============================================" << std::endl; 

  std::cout << "=============Zset migrate done================" << std::endl;
  std::cout << "Total Zset members num: " << num_zset_key << std::endl;
  std::cout << "==============================================" << std::endl; 

  std::cout << "=============List migrate done================" << std::endl;
  std::cout << "Total list members num: " << num_queue_key << std::endl;
  std::cout << "==============================================" << std::endl; 
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tssdb_to_pika read data from SSDB and send to Nemo or Blackwidow" << std::endl;
  std::cout << "\t--host            ssdb_server_ip" << std::endl;
  std::cout << "\t--port            ssdb_server_port" << std::endl;
  std::cout << "\t--path            pika db's path"<< std::endl;
  std::cout << "\t--form            nemo or blackwidow based on your pika version" << std::endl;
  std::cout << "\t--[password]      ssdb_server_password if it has password" << std::endl;
  std::cout << "\t--[n]		    the number of threads write data to db, default is 15" << std::endl;
  std::cout << "\texample: ./ssdb_to_pika 127.0.0.1 8888 ./db blackwidow  password 15" << std::endl;
}
void printInfo(DBType type, const std::time_t& now, std::string& ip, int port, std::string& path) {
  if (type == NEMO){  
    std::cout << "=============ssdb to nemo=====================" << std::endl; } 
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
  if (argc != 5 && argc != 6 && argc != 7) {
    Usage();
    return -1;
  }
  std::string ip = argv[1];
  int port = atoi(argv[2]);
  std::string path = argv[3];
  std::string mode = argv[4];
  std::string password = "";
  int threadnum = 10;
  if (argc == 6) {
    password = argv[5];
  }
  if (argc == 7) {
    threadnum = atoi(argv[6]);
  }
  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  std::atomic<int> num_kv_key(0), num_hash_key(0), num_zset_key(0), num_queue_key(0);
  {
    ThreadPool dispatcher(threadnum);
    if (mode == "nemo") {
      nemo::Options option;
      option.write_buffer_size = 268435456; //256M
      option.target_file_size_base = 20971520; //20M
      option.max_background_flushes = 4;
      option.max_background_compactions = 4;
      std::shared_ptr<nemo::Nemo> db = std::make_shared<nemo::Nemo>(path, option);
      printInfo(NEMO, now, ip, port, path);
      std::thread thread_kv = std::thread(NemoDoKv, ip, port, password, 
                                 db, std::ref(dispatcher), std::ref(num_kv_key));
      std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
      std::thread thread_hash = std::thread(NemoDoHash, ip, port, password, 
                                 db, std::ref(dispatcher), std::ref(num_hash_key)); 
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::thread thread_zset = std::thread(NemoDoZset, ip, port, password, 
                                 db, std::ref(dispatcher), std::ref(num_zset_key));
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::thread thread_queue = std::thread(NemoDoQueue, ip, port, password, 
                                 db, std::ref(dispatcher), std::ref(num_queue_key));
      thread_kv.join();
      thread_hash.join();
      thread_zset.join();
      thread_queue.join();
    } else if (mode == "blackwidow") {
      blackwidow::BlackwidowOptions option;
      std::shared_ptr<blackwidow::BlackWidow> db = std::make_shared<blackwidow::BlackWidow>();
      option.options.create_if_missing = true;
      db->Open(option, path);
      printInfo(BLACKWIDOW, now, ip, port, path); 
      std::thread thread_kv = std::thread(BlackwidowDoKv, ip, port, password, 
                              db, std::ref(dispatcher), std::ref(num_kv_key));
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::thread thread_hash = std::thread(BlackwidowDoHash, ip, port, password, 
                              db, std::ref(dispatcher), std::ref(num_hash_key));
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::thread thread_zset = std::thread(BlackwidowDoZset, ip, port, password, 
                              db, std::ref(dispatcher), std::ref(num_zset_key));
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::thread thread_queue = std::thread(BlackwidowDoQueue, ip, port, password, 
                              db, std::ref(dispatcher), std::ref(num_queue_key));
      thread_kv.join();
      thread_hash.join();
      thread_zset.join();
      thread_queue.join();

    } else {
      Usage();
      return -1;
    }
  }
  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  PrintRes(num_kv_key, num_hash_key, num_zset_key, num_queue_key);
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
