# -*- coding: utf-8 -*-
import scrapy
import re
import json
import redis
import pymongo
from caiji.items import CaijiItem
from scrapy.conf import settings


class A58tongchengSpider(scrapy.Spider):
    name = 'tongcheng58'
    allowed_domains = ['58.com']
    start_urls = ['https://www.58.com/changecity.html']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        redis_host = spider.settings['REDIS_HOST']
        redis_port = spider.settings['REDIS_PORT']
        redis_db = spider.settings['REDIS_DB']
        redis_password = spider.settings['REDIS_PASS']
        mongo_host = spider.settings['MONGO_HOST']
        mongo_port = spider.settings['MONGO_PORT']
        mongo_db = spider.settings['MONGO_DB']
        spider.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
        spider.db = pymongo.MongoClient(host=mongo_host, port=mongo_port)[mongo_db]
        return spider

    def parse(self, response):
        res = response.text
        independent_city = re.search("independentCityList = (.+?)}", res, re.S).group(1) + "}"
        cities = re.search("cityList = (.+?)<\/script>", res, re.S).group(1)
        city_codes = json.loads(cities)
        independent_city_codes = json.loads(independent_city)
        city_codes.pop("其他")
        city_codes.pop("海外")
        for province in city_codes:
            for city in city_codes[province]:
                code = city_codes[province][city].split("|")[0]
                url = "https://{code}.58.com/xiaoqu".format(code=code)
                yield scrapy.Request(url=url, callback=self.parse_district, meta={'province': province, 'city': city.strip()})
        for indep_city in independent_city_codes:
            in_code = independent_city_codes[indep_city].split("|")[0]
            in_url = "https://{code}.58.com/xiaoqu".format(code=in_code)
            yield scrapy.Request(url=in_url, callback=self.parse_district, meta={'province': "", 'city': indep_city.strip()})

    def parse_district(self, response):
        province = response.meta['province']
        city = response.meta['city']
        districts = response.xpath("//dl[@class='secitem']")[0].xpath("./dd/a")
        for district in districts:
            district_name = district.xpath("./text()").extract_first()
            if district_name != "不限":
                district_code = district.xpath("./@value").extract_first()
                district_url = response.url + district_code
                yield scrapy.Request(url=district_url, callback=self.parse_street, meta={'province': province, 'city': city, 'district': district_name.strip()})

    def parse_street(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        streets = response.xpath("//div[@id='qySelectSecond']/a")
        for street in streets:
            street_name = street.xpath("./text()").extract_first()
            street_code = street.xpath("./@value").extract_first()
            street_url = re.search("(.+xiaoqu/)\d+/", response.url).group(1) + street_code
            yield scrapy.Request(url=street_url, callback=self.parse_list, meta={'province': province, 'city': city, 'district': district, 'street': street_name.strip()})

    def parse_list(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        xiaoqu_list = response.xpath("//div[@class='list-info']/h2/a")
        if xiaoqu_list:
            for xiaoqu in xiaoqu_list:
                housing_url = xiaoqu.xpath("./@href").extract_first()
                if not self.redis.sismember(A58tongchengSpider.name, housing_url):
                    yield scrapy.Request(url=housing_url, callback=self.parse_detail, meta={'province': province, 'city': city, 'district': district, 'street': street})
        else:
            # 返回列表为空
            pass

    def parse_detail(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        housing_url = response.url
        items = CaijiItem()
        housing_name = response.xpath("//div[@class='title-bar']/span[@class='title']/text()").extract_first()
        housing_price_raw = response.xpath("//div[@class='price-container']")
        if housing_price_raw:
            price = housing_price_raw[0].xpath("./span[@class='price']/text()").extract_first()
            unit = housing_price_raw[0].xpath("./span[@class='unit']/text()").extract_first()
            housing_price = price + " " + unit
        else:
            housing_price = ""
        info_tb = response.xpath("//table[@class='info-tb']/tr")
        data_dict = {}
        for data in info_tb:
            td_block = data.xpath("./td")
            if len(td_block) == 2:
                key = td_block[0].xpath("./text()").extract_first()
                value = td_block[1].xpath("./@title").extract_first()
                if key not in data_dict:
                    data_dict[key] = value
            if len(td_block) == 4:
                key1 = td_block[0].xpath("./text()").extract_first()
                value1 = td_block[1].xpath("./@title").extract_first()
                if key1 not in data_dict:
                    data_dict[key1] = value1
                key2 = td_block[2].xpath("./text()").extract_first()
                value2 = td_block[3].xpath("./@title").extract_first()
                if key2 not in data_dict:
                    data_dict[key2] = value2
        for label in data_dict:
            if label == "商圈区域":
                items['business_circle'] = data_dict[label]
            if label == "详细地址":
                items['housing_address'] = data_dict[label]
            if label == "建筑类别":
                items['building_type'] = data_dict[label]
            if label == "总住户数":
                items['house_total'] = data_dict[label]
            if label == "产权类别":
                items['property_type'] = data_dict[label]
            if label == "物业费用":
                items['property_fee'] = data_dict[label]
            if label == "产权年限":
                items['right_years'] = data_dict[label]
            if label == "容积率":
                items['capacity_rate'] = data_dict[label]
            if label == "建筑年代":
                items['built_year'] = data_dict[label]
            if label == "绿化率":
                items['greening_rate'] = data_dict[label]
            if label == "占地面积":
                items['area'] = data_dict[label]
            if label == "建筑面积":
                items['building_area'] = data_dict[label]
            if label == "停车位":
                items['parking_place'] = data_dict[label]
            if label == "物业公司":
                items['property_company'] = data_dict[label]
            if label == "开发商":
                items['developer'] = data_dict[label]
        items['province'] = province
        items['city'] = city
        items['district'] = district
        items['street'] = street
        items['housing_url'] = housing_url
        items['housing_name'] = housing_name
        items['housing_price'] = housing_price
        yield items



