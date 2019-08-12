#include <iostream>
#include <unistd.h> 
#include <string>

#include "fde.h"
#include "link.h"
#include "slave.h"
#include "ssdb_impl.h"
#include "hiredis.h"

std::string Slave::EXPIRE = "\xff\xff\xff\xff\xff|EXPIRE_LIST|KV";


Slave::Slave(const char* ssdb_ip, int ssdb_port, const char* pika_ip,
             int pika_port, const char* ssdb_auth, const char* pika_auth){
  thread_quit = false;
  this->recv_timeout = 30;
  this->status = DISCONNECTED;
  this->ssdb_ip = std::string(ssdb_ip);
  this->ssdb_port = ssdb_port;
  if (ssdb_auth == nullptr) {
    this->ssdb_auth = "";
  } else {
    this->ssdb_auth = std::string(ssdb_auth);
  }
  this->pika_ip = pika_ip;
  this->pika_port = pika_port;
  this->pika_auth = pika_auth;
  conn = nullptr;
  reply = nullptr;
  this->is_mirror = false;;
  this->log_type = BinlogType::SYNC;
  this->link = NULL;
  this->last_seq = 0;
  this->last_key = "";
  this->connect_retry = 0;
  this->copy_count = 0;
  this->sync_count = 0;
  this->run_thread_tid = 0;
  createLocalDB();
  connectToPika();
}

void Slave::createLocalDB() {
  Options defaultoptions;
  std::string data_db = "./DB/data";
  std::string meta_db = "./DB/meta";
  while (1) { 
    ssdb = SSDB::open(defaultoptions, data_db);
    if (ssdb) {
      break;
    }
    std::cout << "can't create data DB, try again" << std::endl;
  }
  while (1) {
    meta = SSDB::open(defaultoptions, meta_db); 
    if (meta) {
      break;  
    }
    std::cout << "can't create meta DB, try again" << std::endl;
  }
  std::cout << "creat local DB successfully" << std::endl;
}

Slave::~Slave(){
  if (!thread_quit){
    stop();
  }
  if (link){
    delete link;
  }
}

void Slave::set_id(const std::string &id) { 
  this->id_ = id;
}

void Slave::migrate_old_status() {
  std::string old_key = "new.slave.status|" + this->id_;
  std::string val;
  int old_found = meta->raw_get(old_key, &val);
  if (!old_found) {
    return;
  } 
  if (val.size() < sizeof(uint64_t)) {
    std::cout << "invalid format of status" << std::endl;
  }
  last_seq = *((uint64_t*)(val.data()));
  last_key.assign(val.data() + sizeof(uint64_t), val.size() - sizeof(uint64_t));
  save_status();
  if (meta->raw_del(old_key) == -1) {
    std::cout << "meta db error" << std::endl;
    exit(1);
  }
}

void Slave::load_status() {
  std::string key;  
  std::string seq;
  meta->hget(status_key(), "last_key", &key);
  meta->hget(status_key(), "last_seq", &seq);
  if (!key.empty()) {
    this->last_key = key;
  }
  if (!seq.empty()) {
    this->last_seq = str_to_int64(seq);
  }
}

void Slave::save_status() {
  std::string seq = str(this->last_seq);
  meta->hset(status_key(), "last_key", this->last_key);
  meta->hset(status_key(), "last_seq", seq);
}

std::string Slave::status_key() {
  std::string key;
  key = "slave.status." + this->id_;
  return key;
}

void Slave::start(){
  if (thread_quit) {
    return;
  }
  migrate_old_status();
  if (last_seq != 0 || !last_key.empty()) {
    save_status();
  } else {
    load_status();
  }
  pthread_create(&run_thread_tid, NULL, &Slave::_run_thread, this);
}

void Slave::connectToPika() {
  if (conn) {
    reply = (redisReply*)redisCommand(conn, "ping");
    if (reply && !strcmp(reply->str, "PONG")) {
      freeReplyObject(reply);
      reply = nullptr; 
      return;
    }
  }
  std::cout << "try connectToPika" << std::endl;
  while (1) {
    if (conn) {
      redisFree(conn);
      conn = nullptr;
    }
    conn = redisConnect(pika_ip, pika_port);
    if (!conn || conn->err) {
      sleep(5);
      if (conn) {
        redisFree(conn);
        conn = nullptr;
        std::cout << "connection error retry!" << std::endl;
      } else {
        std::cout << "conneciont error : can't allocate redis context. retry!" << std::endl;
      }
      continue;
    } 
    if (this->pika_auth != nullptr) {
      reply = (redisReply*)redisCommand(conn, "AUTH %s", pika_auth);
      if (reply->type == REDIS_REPLY_ERROR) {
        std::cout << "authentication failed. quit!" << std::endl; 
        freeReplyObject(reply);
        reply = nullptr;
        redisFree(conn);
        conn = nullptr;
        thread_quit = true;
      } else {
        std::cout << "authentication success " << std::endl; 
        freeReplyObject(reply);
        reply = nullptr;
      }   
    } 
    break;
  }
  std::cout << "connect to pika successful" << std::endl;  
}

void Slave::stop(){
  thread_quit = true;
  void *tret;
  int err = pthread_join(run_thread_tid, &tret);
  if (err != 0) {
    std::cout << "can't join thread" << std::endl;
  }
}

int Slave::connect(){
  const char *ip = this->ssdb_ip.c_str();
  int port = this->ssdb_port; 
  if (++connect_retry % 50 == 1){
    link = Link::connect(ip, port);
    if (link == NULL){
      std::cout << "connect to ssdb server failed!!!" << std::endl;
      goto err;
    } else{
      status = INIT;
      connect_retry = 0;
      const char *type = is_mirror? "mirror" : "sync";
			
      if (!this->ssdb_auth.empty()){
	const std::vector<Bytes> *resp;
	resp = link->request("auth", this->ssdb_auth);
	if (resp->empty() || resp->at(0) != "ok"){
          std::cout << "authorzation to ssdb-server is denied!!!" << std::endl;
	  delete link;
	  link = NULL;
	  sleep(1);
	  return -1;
	}
      }
			
      link->send("sync140", str(this->last_seq), this->last_key, type);
      if (link->flush() == -1){
        std::cout <<"[" << this->id_ << "]" <<  " network error" << std::endl;
	delete link;
	link = NULL;
	goto err;
      }
      std::cout << "ready to receive binlogs" << std::endl;
      return 1;
    }
  } 
  return 0;
err:
  return -1;
}

void* Slave::_run_thread(void *arg){
  Slave *slave = (Slave *)arg;
  const std::vector<Bytes> *req;
  Fdevents select;
  const Fdevents::events_t *events;
  int idle = 0;
  bool reconnect = false;
	
#define RECV_TIMEOUT		200
  int max_idle = (slave->recv_timeout * 1000) / RECV_TIMEOUT;

  while (!slave->thread_quit) {
    if (reconnect) {
      slave->status = DISCONNECTED;
      reconnect = false;
      select.del(slave->link->fd());
      delete slave->link;
      slave->link = NULL;
      sleep(1);
    }
    if (!slave->connected()) {
      if (slave->connect() != 1){
	usleep(100 * 1000);
      } else {
	select.set(slave->link->fd(), FDEVENT_IN, 0, NULL);
      }
      continue;
    }
		
    events = select.wait(RECV_TIMEOUT);
    if (events == NULL) {
      std::cout << "events.wait error" << std::endl;
      sleep(1);
      continue;
    } else if (events->empty()) {
      if (idle++ >= max_idle) {
        std::cout << "the master hasn't responsed for a while, reconnect..." << std::endl;
	idle = 0;
	reconnect = true;
      }
      continue;
    }
    idle = 0;

    if (slave->link->read() <= 0) {
      std::cout << "link.read error, reconnecting to ssdb server" << std::endl;
      reconnect = true;
      continue;
    }

    while (1) {
      req = slave->link->recv();
      if (req == NULL) {
        std::cout << "link.recv error, reconnecting to ssdb server" << std::endl;
	reconnect = true;
	break;
      } else if (req->empty()) {
	break;
      } else if (req->at(0) == "noauth") {
	reconnect = true;
	sleep(1);
	break;
      } else {
	if (slave->proc(*req) == -1) {
	  goto err;
        }
      }
    }
  } // end while

  std::cout << "ssdb_to_pika tools quit" << std::endl;
  return (void *)NULL;
err:
  std::cout << "ssdb_to_pika tools exit unexpectedlhy" << std::endl;
  exit(0);
  return (void *)NULL;;
}

int Slave::proc(const std::vector<Bytes> &req){
  Binlog log;
  if (log.load(req[0]) == -1) {
    std::cout << "invalid binlog!" << std::endl;
    return 0; 
  }
  const char *sync_type = this->is_mirror? "mirror" : "sync";
  switch (log.type()) {

    case BinlogType::NOOP:
      return this->proc_noop(log, req);
      break;

    case BinlogType::CTRL:
    if (log.key() == "OUT_OF_SYNC") {
      status = OUT_OF_SYNC;
      std::cout << "out_of_sync, you must run this tools again manually" << std::endl;
    }
    break;

    case BinlogType::COPY: {
      status = COPY;
      this->proc_copy(log, req);
      break;
    }

    case BinlogType::SYNC:

    case BinlogType::MIRROR: {
      status = SYNC;
      if (++sync_count % 1000 == 1) {
        std::cout << "sync_count: " << sync_count << ", last_seq: " << this->last_seq << std::endl;
      }
      this->proc_sync(log, req);
      break;
    }

    default:
      break;
  }
  return 0;
}

int Slave::proc_noop(const Binlog &log, const std::vector<Bytes> &req){ 
  uint64_t seq = log.seq();
  if (this->last_seq != seq) {
    this->last_seq = seq;
    this->save_status();
  }
  return 0;
}

int Slave::proc_copy(const Binlog &log, const std::vector<Bytes> &req){
  switch (log.cmd()){

    case BinlogCommand::BEGIN:
      std::cout<< "copy begin " << std::endl;
      break;

    case BinlogCommand::END:
      std::cout << "copy end " << ", copy_count: " << copy_count 
                               << ", last_seq: " << this->last_seq<< std::endl;
      this->status = SYNC;
      this->last_key = "";
      this->save_status();
      break;

    default:
      if (++copy_count % 1000 == 1){
        std::cout << "copy_count: " << copy_count << ", last_seq: " << this->last_seq << std::endl;
      }
      return proc_sync(log, req);
      break;
  }
  return 0;
}

int Slave::proc_sync(const Binlog &log, const std::vector<Bytes> &req){
  switch(log.cmd()) {

    case BinlogCommand::KSET: {
      if (req.size() != 2) {
	break;
      }
      std::string key;
      if (decode_kv_key(log.key(), &key) == -1) {
	break;
      }
      if (ssdb->set(key, req[1], log_type) == -1) {
        return -1;
      } 
      std::string cmd = "set ";
      cmd += key;    
      cmd += " ";
      cmd += req[1].String();
      if (this->status == SYNC) {
        connectToPika();
      }
      reply = (redisReply*)redisCommand(conn, cmd.c_str());  
      if (reply != nullptr) {
        freeReplyObject(reply);
        reply = nullptr;
      } else {
        return -1;
      }
    }
    break;

    case BinlogCommand::KDEL: {
      std::string key;
      if (decode_kv_key(log.key(), &key) == -1) {
	break;
      }
      if (ssdb->del(key, log_type) == -1) {
        return -1;
      } 
      std::string cmd = "del ";
      cmd += key;
      if (this->status == SYNC) {
        connectToPika();
      }
      reply = (redisReply*)redisCommand(conn, cmd.c_str());
      if (reply != nullptr) {
        freeReplyObject(reply);
        reply = nullptr;
      } else {
        return -1;
      }
    }
    break;

    case BinlogCommand::HSET: {
      if (req.size() != 2) {
	break;
      }
      std::string name, key;
      if (decode_hash_key(log.key(), &name, &key) == -1) {
        break;
      }
      if (ssdb->hset(name, key, req[1], log_type) == -1) {
        return -1;
      }
      std::string cmd; 
      cmd = "hset ";
      cmd += name + " " + key + " ";
      cmd += req[1].String();
      if (this->status == SYNC) {
        connectToPika();
      }
      reply = (redisReply*)redisCommand(conn, cmd.c_str());
      if (reply != nullptr) {
        freeReplyObject(reply); 
        reply = nullptr;
      } else {
        return -1;
      } 
      break; 
    }

    case BinlogCommand::HDEL: {
      std::string name, key;
      if (decode_hash_key(log.key(), &name, &key) == -1) {
	break;
      }
      if (ssdb->hdel(name ,key ,log_type) == -1) {
        return -1;
      }
      std::string cmd = "hdel ";
      cmd += name + " ";
      cmd += key;
      if (this->status == SYNC) {
        connectToPika();
      }
      reply = (redisReply*)redisCommand(conn, cmd.c_str());
      if (reply != nullptr) {
        freeReplyObject(reply);
        reply = nullptr;
      } else {
        return -1;
      }
    }
    break;

    case BinlogCommand::ZSET: {
      if (req.size() != 2) {
	break;
      }
      std::string cmd = "zadd ";
      std::string name, key;
      if (decode_zset_key(log.key(), &name, &key) == -1) {
	break;
      } 
      if (ssdb->zset(name, key, req[1], log_type) == -1) {
        return -1;
      }
      if (name == EXPIRE) {
        int64_t ex = str_to_int64(req[1].String());
        int64_t seconds = (ex - time_ms()) / 1000;
        std::string str_seconds = str(seconds); 
        cmd = "expire "+ key + " " + str_seconds; 
        if (this->status == SYNC) {
          connectToPika();
        }
        reply = (redisReply*)redisCommand(conn, cmd.c_str());
      } else {
        cmd += name + " " + req[1].String() + " " + key;
        if (this->status == SYNC) {
          connectToPika();
        }
        reply = (redisReply*) redisCommand(conn, cmd.c_str());
      }
      if (reply != nullptr) {
        freeReplyObject(reply);
        reply = nullptr;
      } else {
        return -1;
      }
    }
    break;

    case BinlogCommand::ZDEL: {
      std::string cmd = "zrem ";
      std::string name, key;
      if (decode_zset_key(log.key(), &name, &key) == -1) {
	break;
      }
      if (ssdb->zdel(name, key, log_type) == -1) {
        return -1;
      }
      cmd += name + " " + key;
      if (this->status == SYNC) {
        connectToPika();
      }
      reply = (redisReply*)redisCommand(conn, cmd.c_str());
      if (reply != nullptr) {
        freeReplyObject(reply);
        reply = nullptr;
      }
      else {
        return -1;
      }
    }
    break;

    case BinlogCommand::QSET:
    case BinlogCommand::QPUSH_BACK:
    case BinlogCommand::QPUSH_FRONT: {
      if (req.size() != 2) {
        break;
      }
      std::string name;
      uint64_t seq;
      if (decode_qitem_key(log.key(), &name, &seq) == -1) {
        break;
      }
      if (seq < QITEM_MIN_SEQ || seq > QITEM_MAX_SEQ) {
	break;
      }
      int ret;
      if (log.cmd() == BinlogCommand::QSET) {
        ret = ssdb->qset_by_seq(name, seq, req[1], log_type);
        uint64_t front_seq; 
        ssdb->qget_front(name, &front_seq);
        std::string cmd = "lset ";
        cmd += name + " ";
        cmd += str(seq - front_seq) + " " + req[1].String();       
        if (this->status == SYNC) {
          connectToPika();
        }
        reply = (redisReply*)redisCommand(conn, cmd.c_str());
        if (reply != nullptr) {
          free(reply);
          reply = nullptr;
        } else {
          return -1;
        }
      } else if (log.cmd() == BinlogCommand::QPUSH_BACK){
        ret = ssdb->qpush_back(name, req[1], log_type); 
        std::string cmd = "rpush ";
        cmd += name + " " + req[1].String();
        if (this->status == SYNC) {
          connectToPika();
        }
        reply = (redisReply*) redisCommand(conn, cmd.c_str());
        if (reply != nullptr) {
          freeReplyObject(reply);
          reply = nullptr;
        } else {
          return -1;
        }
      } else {
          ret = ssdb->qpush_front(name, req[1], log_type);
          uint64_t front_seq;
          ssdb->qget_front(name, &front_seq);
          std::string cmd = "lpush ";
          cmd += name + " " + req[1].String();
          if (this->status == SYNC) {
            connectToPika();
          }
          reply = (redisReply*) redisCommand(conn, cmd.c_str());
          if (this->status == SYNC) {
            connectToPika();
          }
          if (reply != nullptr) {
          freeReplyObject(reply);
          reply = nullptr;
          }
      } 
      if (ret == -1) {
        return -1;
      }    
    }
    break;

    case BinlogCommand::QPOP_BACK:
    case BinlogCommand::QPOP_FRONT:{
      int ret;
      const Bytes name = log.key();
      std::string tmp;
      if (log.cmd() == BinlogCommand::QPOP_BACK){
        ret = ssdb->qpop_back(name, &tmp, log_type); 
        std::string cmd = "rpop " + name.String();
        if (this->status == SYNC) {
          connectToPika();
        }
        reply = (redisReply*)redisCommand(conn, cmd.c_str());
        if (reply != nullptr) {
          freeReplyObject(reply);
          reply = nullptr;
        } else {
          return -1;
        }
      } else {
        ret = ssdb->qpop_front(name, &tmp, log_type); 
        std::string cmd = "lpop " + name.String();
        if (this->status == SYNC) {
          connectToPika();
        }
        reply = (redisReply*)redisCommand(conn, cmd.c_str());
        if (reply != nullptr) {
          freeReplyObject(reply);
          reply = nullptr;
        } else {
          return -1;
        }
      }
    }
    break;
 
    default:
      std::cout << "unknow binlog, type=" << log.type() << ", cmd=" << log.cmd() << std::endl;
      break;
  }
  this->last_seq = log.seq();
  if (log.type() == BinlogType::COPY) {
    this->last_key = log.key().String();
  } 
  return 0;
}

