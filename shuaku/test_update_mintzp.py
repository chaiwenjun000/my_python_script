##!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
import json
import requests
import time
import threading # 多线程
import multiprocessing  # 多进程
import queue
import random
import logging

logging.basicConfig(level=10,
			format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
             filename=r'log.log')      # filename 是将信息写入 log.log  文件中
succeed = 0
fail_status_code = []
fail_errno = []


'''
    读取所有特征
'''
def get_features():

    succeed_dict = {}

    with open('succeed.txt','r') as f:
        all = f.read().split()
        for a in all:
            succeed_dict[a] = 1

    features = []
    cnt = 0
    start = time.time()
    #pool = multiprocessing.Pool(processes=5)
    with open('get_feature_for_video_shuaku_step2_merge_wj_nody','r') as f: # rb加速读
        lines = f.read().splitlines()
        for line in lines:
            if line.strip() == "":
                continue
            #print(line)
            feature = json.loads(json.dumps(eval(line)))

            if feature['tid'] in succeed_dict: # 去除已经成功更新的。
                continue
            features.append(feature)

            #print(features)
            cnt += 1 
    
    # 1938644 未曾筛选的数量 1922546 筛选之后

    end = time.time()
    print('feature cnt: ' , cnt, 'get_features cost : ',end - start)
    return features
# def get_features():
#     features = []
#     cnt = 0
#     start = time.time()
#     with open('get_feature_for_video_shuaku_step2_merge_wj_nody','rb') as f: # rb加速读
#         for line in f:
#             if line.strip() == "":
#                 continue
#             li  = line.strip()
#             lin = str(li).lstrip("b")
#             lin = lin[1:-1] # 去掉收尾双引号
#             lin = lin.replace('\'','\"') # 转换为" json.loads好读取
#             features.append(lin)
#             cnt += 1 

#     end = time.time()
#     print('feature cnt: ' , cnt, 'get_features cost : ',end - start)
#     logging.info('feature cnt: ' + str(cnt)+' get_features cost : ' + str(end - start))
#     logging.info(str(features[:10]))
#     return features
   

'''
    get_instance_by_service -ip yuelaou-1841.orp.all > ip.txt
    读取可用ip:port 
'''
def get_ips():
    ips = []
    with open('ip.txt','r') as f: # 不用二进制加速
        for line in f:
            if line.strip() == "":
                continue
            li  = line.strip()
            tmp = li.split(' ')
            ips.append((tmp[1],tmp[2]))
    logging.info(str(ips))
    return ips
    


# get_ips()



def main():

    features = get_features()
    ips = get_ips()
    # print(len(features))
    # print(ips[random.randint(0,615)])
    # print(len(ips))
    globaldict = {}

    # 通过队列控制多线程并发。
    class store(threading.Thread):
        def __init__(self, params, queue):
            threading.Thread.__init__(self)
            self.queue = queue
            self.params = params

        def run(self):
            try:
                ret = requests.get(url,params = self.params)
                # print(ret.url) # 查看拼接的字符串
                logging.info(ret.url)
                # print(self.params['thread_id'],ret,ret.elapsed.total_seconds())
                if ret.status_code == 200:
                    ans = json.loads(ret.text)
                    if ans['errno'] == 0:
                        logging.info(str(self.params['thread_id']) +str(ret.text) + str(ret.elapsed.total_seconds() ) ) # 正常信息
                        with open('succeed.txt','a') as f:
                            f.write(self.params['thread_id'] + ' ')
                    else:
                        logging.error(str(self.params['thread_id']) +str(ret.text) + str(ret.elapsed.total_seconds() ) ) 
                        fail_errno.append(self.params['thread_id'])
                        with open('error1.txt','a') as f:
                            f.write(self.params['thread_id'] + ' ')
                            
                else:
                    logging.error(str(self.params['thread_id']) +str(ret.text) + str(ret.elapsed.total_seconds() ) ) # 正常信息
                    fail_status_code.append(self.params['thread_id'])
                    with open('error2.txt','a') as f:
                            f.write(self.params['thread_id'] + ' ')
                # global globaldict
                # if self.params['thread_id'] in globaldict:
                #     globaldict[self.params['thread_id']] += 1
                # else:
                #     globaldict[self.params['thread_id']] = 1
            except Exception as e:
                print(e)
                logging.error(str(e))
            finally:
                self.queue.get() # 出队
                self.queue.task_done() # 通知线程结束

    # exit(0)
    
    cnt = 0
    #pool = multiprocessing.Pool(processes=5) # 5核心 10qps  io密集型，应该用多线程

    maxThreads = 5
    q = queue.Queue(maxThreads)

    start = time.time()

    local_dict = {}
    for featured in features:
        ip = ips[random.randint(0,615)]
        url = 'http://'+ip[0]+':'+ip[1]+'/service/inbound?method=updateMultiVideoFeature'
        s_start = time.time()
        #feature = json.loads(featured)
        feature = featured
        params = {
            "thread_id":feature['tid'],
            'feature':json.dumps({  # python dict对象 编码成json字符串
                "eff_t":feature["eff_t"],
                "c_q":feature["c_q"],
                "cover_clarity":feature["cover_clarity"],
                "title_q":feature["title_q"],
                "author_brand":feature["author_brand"],
            }),
            "need_odyssey_jump":1,
            "ie":"utf-8",
            "format":"json"
        }
        logging.info('current number : ' + str(cnt))
        # if params['thread_id'] in local_dict:
        #     local_dict[params['thread_id']] += 1
        # else:
        #     local_dict[params['thread_id']] = 1
        q.put(params)
        t = store(params,q)
        t.start()
        cnt += 1
        # if cnt == 100:
        #     break
    q.join()

    end = time.time()
    print(cnt,'all cost : ',end - start)  

    logging.info('all to feature inserted: '+str(cnt) + ' all cost : '+ str(end - start))
    print('fail status_code: ' + ' '.join(fail_status_code))
    print('fail errno: ' + ' '.join(fail_errno))

    logging.info('fail status_code: ' + ' '.join(fail_status_code))
    logging.info('fail errno: ' + ' '.join(fail_errno))


main()