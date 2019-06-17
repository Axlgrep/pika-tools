
#include "redis_sender.h"

#include <time.h>
#include <unistd.h>

#include <glog/logging.h>

#include "slash/include/xdebug.h"

static time_t kCheckDiff = 1;

RedisSender::RedisSender(int id, std::string ip, int64_t port, std::string password):
  id_(id),
  cli_(NULL),
  rsignal_(&commands_mutex_),
  wsignal_(&commands_mutex_),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  elements_(0) {

  last_write_time_ = ::time(NULL);
}

RedisSender::~RedisSender() {
  LOG(INFO) << "RedisSender thread " << id_ << " exit!!!";
}

void RedisSender::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(1000);
    cli_->set_recv_timeout(10000);
    cli_->set_send_timeout(10000);
    slash::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      cli_ = NULL;
      LOG(WARNING) << "RedisSender " << id_ << " Can not connect to "
        << ip_ << ":" << port_ << ", status: " << s.ToString();
      sleep(3);
      continue;
    } else {
      // Connect success
      LOG(INFO) << "RedisSender " << id_ << " Connect to " << ip_ << ":" << port_ << " success";

      // Authentication
      if (!password_.empty()) {
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("AUTH");
        argv.push_back(password_);
        pink::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (resp[0] == "OK") {
            LOG(INFO) << "RedisSender " << id_ << " Authentic success";
          } else {
            cli_->Close();
            cli_ = NULL;
            should_exit_ = true;
            LOG(WARNING) << "RedisSender " << id_ << " Invalid password";
            return;
          }
        } else {
          cli_->Close();
          LOG(INFO) << "RedisSender " << id_ << " Recv response error: " << s.ToString();
          delete cli_;
          cli_ = NULL;
          continue;
        }
      } else {
        // If forget to input password
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        pink::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              cli_->Close();
              LOG(WARNING) << "RedisSender " << id_ << " Authentication required";
              cli_ = NULL;
              should_exit_ = true;
              return;
            }
          } else {
            cli_->Close();
            LOG(INFO) << "RedisSender " << id_ << " Recv response error: " << s.ToString();
            delete cli_;
            cli_ = NULL;
          }
        }
      }
    }
  }
}

void RedisSender::Stop() {
  set_should_stop();
  should_exit_ = true;
  commands_mutex_.Lock();
  rsignal_.Signal();
  commands_mutex_.Unlock();
}

void RedisSender::SendRedisCommand(const std::string &command) {
  commands_mutex_.Lock();
  if (commands_queue_.size() < 100000) {
    commands_queue_.push(command);
    rsignal_.Signal();
    commands_mutex_.Unlock();
    return;
  }

  //LOG(WARNING) << id_ << "commands queue size is beyond 100000";
  while (commands_queue_.size() > 100000) {
    wsignal_.Wait();
  }
  commands_queue_.push(command);
  rsignal_.Signal();
  commands_mutex_.Unlock();
}

int RedisSender::SendCommand(std::string &command) {
  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
    bool alive = true;
    pink::RedisCmdArgsType argv, resp;
    std::string ping;
    argv.push_back("PING");
    pink::SerializeRedisCommand(argv, &ping);
    slash::Status s = cli_->Send(&ping);
    if (s.ok()) {
      s = cli_->Recv(&resp);
      if (!s.ok() || resp.size() != 1 || strcasecmp(resp[0].data(), "pong")) {
        alive = false;
      }
    } else {
      alive = false;
    }

    if (!alive) {
      LOG(WARNING) << "RedisSender " << id_ << " Timeout disconnect, try to reconnect";
      cli_->Close();
      cli_ = NULL;
      ConnectRedis();
    }
  }

  // Send command
  int idx = 0;
  do {
    slash::Status s = cli_->Send(&command);
    if (s.ok()) {
      s = cli_->Recv(NULL);
      if (s.ok()) {
        last_write_time_ = now;
        return 0;
      }
    }

    LOG(WARNING) << "RedisSender " << id_ << " Fails to send redis command "
      << command << ", times: " << idx + 1 << ", status: " << s.ToString();
    cli_->Close();
    delete cli_;
    cli_ = NULL;
    ConnectRedis();
  } while(++idx < 3);

  return -1;
}

void *RedisSender::ThreadMain() {
  LOG(INFO) << "Start RedisSender " << id_ << " Thread...";

  ConnectRedis();

  while (!should_exit_) {
    commands_mutex_.Lock();
    while (commands_queue_.size() == 0 && !should_exit_) {
      rsignal_.TimedWait(100);
      // rsignal_.Wait();
    }
    // if (commands_queue_.size() == 0 && should_exit_) {
    if (should_exit_) {
      commands_mutex_.Unlock();
      break;
    }

    if (commands_queue_.size() == 0) {
      commands_mutex_.Unlock();
      continue;
    }
    commands_mutex_.Unlock();

    // get redis command
    std::string command;
    commands_mutex_.Lock();
    command = commands_queue_.front();
    // printf("%d, command %s\n", id_, command.c_str());
    elements_++;
    commands_queue_.pop();
    wsignal_.Signal();
    commands_mutex_.Unlock();
    SendCommand(command);
  }
  delete cli_;
  cli_ = NULL;
  LOG(INFO) << "RedisSender Thread " << id_ << " Complete";
  return NULL;
}

