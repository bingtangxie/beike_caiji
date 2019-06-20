# import pymongo
# import redis
#
#
# client = pymongo.MongoClient(host='127.0.0.1', port=27017)['xiaoqucaiji_test']
# c = client['fangtianxia']
# rds = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, password=None)
# res = c.find({}, {"housing_url": 1})
# sum = 0
# for item in res:
#     rds.sadd('fangtianxia', item['housing_url'])
#     sum += 1
#     print(sum)
#
