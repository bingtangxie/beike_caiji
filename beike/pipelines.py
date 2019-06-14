# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.conf import settings
import pymongo
import redis
from datetime import datetime


class BeikePipeline(object):
    def __init__(self):
        host = settings['MONGO_HOST']
        port = settings["MONGO_PORT"]
        dbname = settings["MONGO_DB"]
        client = pymongo.MongoClient(host=host, port=port)
        mydb = client[dbname]
        self.db = mydb

        self.redis = redis.StrictRedis(host=settings['REDIS_HOST'], port=settings['REDIS_PORT'],
                                       db=settings['REDIS_DB'], password=settings['REDIS_PASS'])

    def process_item(self, item, spider):
        data = dict(item)
        data['created'] = datetime.now()
        if not self.redis.sismember(spider.name, data['housing_id']):
            self.db[spider.name].insert(data)
            self.redis.sadd(spider.name, data['housing_id'])
        else:
            print(data['housing_id'])
        # print("正在插入数据: ", data)
        # # self.db[spider.name].insert(data)
        # print(data['housing_id'])
