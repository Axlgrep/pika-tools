// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// #include <glog/logging.h>
#include <random>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "log.h"
#include "pika_port.h"
#include "conf.h"

Conf g_conf;
PikaPort* g_pika_port;
int pidFileFd = 0;

static int lockFile(int fd) {
  struct flock lock;
  lock.l_type = F_WRLCK;
  lock.l_start = 0;
  lock.l_whence = SEEK_SET;
  lock.l_len = 0;

  return fcntl(fd, F_SETLK, &lock);
}

static void createPidFile(const char *file) {
  int fd = open(file, O_RDWR | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR);
  if (-1 == fd) {
    pfatal("open(%s) = %d, error:%m", file, fd);
  }

  int ret = lockFile(fd);
  if (ret < 0) {
    close(fd);
    pfatal("lock(%d) = %d, error:%m", fd, ret);
  }

  // int pid = (int)(getpid());
  pidFileFd = fd;
}

static void daemonize() {
  if (fork() != 0) exit(0); /* parent exits */
  setsid(); /* create a new session */
}

static void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

static void IntSigHandle(const int sig) {
  pinfo("Catch Signal %d, cleanup...", sig);
  if (2 < pidFileFd) {
    close(pidFileFd);
  }
  g_pika_port->Stop();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void Usage()
{
    fprintf(stderr,
            "Usage: pika_port [-h] [-t local_ip -p local_port -i master_ip -o master_port "
            "-m forward_ip -n forward_port -x forward_thread_num -y forward_passwd "
            "-z bgsave-wait-timeout -f filenum -s offset -w password -r rsync_dump_path "
            "-l log_path \n"
            "\t-h     -- show this help\n"
            "\t-t     -- local host ip(OPTIONAL default: 127.0.0.1) \n"
            "\t-p     -- local port(OPTIONAL) \n"
            "\t-i     -- master ip(OPTIONAL default: 127.0.0.1) \n"
            "\t-o     -- master port(REQUIRED) \n"
            "\t-m     -- forward ip(OPTIONAL default: 127.0.0.1) \n"
            "\t-n     -- forward port(REQUIRED) \n"
            "\t-x     -- forward thread num(OPTIONAL default: 1) \n"
            "\t-y     -- forward password(OPTIONAL) \n"
            "\t-z     -- max timeout duration for waiting pika master bgsave data (OPTIONAL default 1800s) \n"
            "\t-f     -- binlog filenum(OPTIONAL default: local offset) \n"
            "\t-s     -- binlog offset(OPTIONAL default: local offset) \n"
            "\t-w     -- password for master(OPTIONAL) \n"
            "\t-r     -- rsync dump data path(OPTIONAL default: ./rsync_dump) \n"
            "\t-l     -- local log path(OPTIONAL default: ./log) \n"
            "\t-b     -- max batch number when port rsync dump data (OPTIONAL default: 512) \n"
            "\t-d     -- daemonize(OPTIONAL) \n"
            "\t-e     -- exit(return -1) if dbsync start(OPTIONAL) \n"
            "  example: ./pika_port -t 127.0.0.1 -p 12345 -i 127.0.0.1 -o 9221 "
            "-m 127.0.0.1 -n 6379 -x 7 -f 0 -s 0 -w abc -l ./log -r ./rsync_dump -b 512 -d -e\n"
           );
}

int main(int argc, char *argv[])
{
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  char c;
  char buf[1024];
  bool is_daemon = false;
  long num = 0;
  while (-1 != (c = getopt(argc, argv, "t:p:i:o:f:s:w:r:l:m:n:x:y:z:b:edh"))) {
    switch (c) {
      case 't':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.local_ip = std::string(buf);
        break;
      case 'p':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.local_port = int(num);
        break;
      case 'i':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.master_ip = std::string(buf);
        break;
      case 'o':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.master_port = int(num);
        break;
      case 'm':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.forward_ip = std::string(buf);
        break;
      case 'n':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.forward_port = int(num);
        break;
      case 'x':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.forward_thread_num = int(num);
        break;
      case 'y':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.forward_passwd = std::string(buf);
        break;
      case 'z':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.wait_bgsave_timeout = time_t(num);
        break;

      case 'f':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.filenum = (size_t)(num);
        break;
      case 's':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.offset = (size_t)(num);
        break;
      case 'w':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.passwd = std::string(buf);
        break;

      case 'r':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.dump_path = std::string(buf);
        if (g_conf.dump_path[g_conf.dump_path.length() - 1] != '/' ) {
          g_conf.dump_path.append("/");
        }
        break;
      case 'l':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.log_path = std::string(buf);
        if (g_conf.log_path[g_conf.log_path.length() - 1] != '/' ) {
          g_conf.log_path.append("/");
        }
        break;
      case 'b':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.sync_batch_num = (size_t)(num);
        break;
      case 'e':
        g_conf.exit_if_dbsync = true;
        break;
      case 'd':
        is_daemon = true;
        break;
      case 'h':
        Usage();
        return 0;
      default:
        Usage();
        return 0;
    }
  }

  if (g_conf.local_port == 0) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> di(10000, 40000);
    // g_conf.local_port = di(mt);
    g_conf.local_port = 21333;
    pinfo("Use random port: %d", g_conf.local_port);
  }

  std::cout << "local_ip:" << g_conf.local_ip << " "
            << ", local_port:" << g_conf.local_port << " "
            << ", master_ip:"  << g_conf.master_ip << " "
            << ", master_port:"  << g_conf.master_port << " "
            << ", forward_ip:"  << g_conf.forward_ip << " "
            << ", forward_port:"  << g_conf.forward_port << " "
            << ", forward_passwd:"  << g_conf.forward_passwd << " "
            << ", forward_thread_num:" << g_conf.forward_thread_num << " "
            << ", wait_bgsave_timeout:"  << g_conf.wait_bgsave_timeout << " "
            << ", log_path:"   << g_conf.log_path << " "
            << ", dump_path:"  << g_conf.dump_path << " "
            << ", filenum:"    << g_conf.filenum << " "
            << ", offset:"     << g_conf.offset << " "
            << ", passwd:"     << g_conf.passwd << " "
            << ", sync_batch_num:"  << g_conf.sync_batch_num << " "
            << std::endl;

  if (g_conf.master_port == 0 || g_conf.forward_port == 0
     || g_conf.sync_batch_num == 0 || g_conf.wait_bgsave_timeout <= 0) {
    fprintf (stderr, "Invalid Arguments\n" );
    Usage();
    exit(-1);
  }

  std::string pid_file_name = "/tmp/pika_port_" + std::to_string(getpid());
  createPidFile(pid_file_name.c_str());

  // daemonize if needed
  if (is_daemon) {
    daemonize();
  }

  SignalSetup();

  g_pika_port = new PikaPort(g_conf.master_ip, g_conf.master_port, g_conf.passwd);
  if (is_daemon) {
    close_std();
  }

  g_pika_port->Start();

  return 0;
}

