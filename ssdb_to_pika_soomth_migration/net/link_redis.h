/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_REDIS_LINK_H_
#define NET_REDIS_LINK_H_

#include <vector>
#include <string>
#include "../util/bytes.h"

struct RedisRequestDesc   //redis请求描述
{
	int strategy;
	std::string redis_cmd;  //redis命令
	std::string ssdb_cmd;  //ssdb命令
	int reply_type;       //回复类型
};

class RedisLink
{
private:
	std::string cmd;   //命令
	RedisRequestDesc *req_desc;  //redis请求描述指针

	std::vector<Bytes> recv_bytes;   //ssdb处理请求时用Bytes数据结构. 但 Bytes本身只是指针
	std::vector<std::string> recv_string; //recv_string来存储接收到的网络请求数据
	int parse_req(Buffer *input);  //解析请求
	int convert_req();  //转换请求
	
public:
	RedisLink(){
		req_desc = NULL;  //初始化redis请求描述为空
	}
	
	const std::vector<Bytes>* recv_req(Buffer *input); //从输入缓冲区获取请求
	int send_resp(Buffer *output, const std::vector<std::string> &resp); //讲请求发送至输入缓冲区
};

#endif
