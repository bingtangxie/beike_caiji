# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
from scrapy.conf import settings
from caiji.items import CaijiItem


class AnjukeSpider(scrapy.Spider):
    name = 'anjuke'
    allowed_domains = ['anjuke.com']
    start_urls = ['https://www.anjuke.com/sy-city.html']

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
        content = response.xpath('//div[@class="letter_city"]/ul/li')
        for block in content:
            label = block.xpath("./label/text()").extract_first()
            cities = block.xpath("./div[@class='city_list']/a")
            if label == "其他":
                pass
            else:
                for city in cities:
                    city_name = city.xpath("./text()").extract_first()
                    city_url = city.xpath("./@href").extract_first()
                    yield scrapy.Request(url=city_url, callback=self.parse_city, meta={'city': city_name})

    def parse_city(self, response):
        # print(response.text)
        header_nav = response.xpath("//ul[@class='L_tabsnew']/li")
        city = response.meta['city']
        if header_nav:
            for header_item in header_nav:
                label = header_item.xpath("./a/text()").extract_first().strip()
                if label == "新 房":
                    xinfang_url = header_item.xpath("./a/@href").extract_first()
                    search_type = "xinfang"
                    yield scrapy.Request(url=xinfang_url, callback=self.parse_district,
                                         meta={'city': city, 'search_type': search_type})
                if label == "二手房":
                    ershoufang_url = header_item.xpath("./a/@href").extract_first()
                    search_type = "ershoufang"
                    yield scrapy.Request(url=ershoufang_url, callback=self.parse_district,
                                         meta={'city': city, 'search_type': search_type})
        else:
            pass

    def parse_district(self, response):
        search_type = response.meta['search_type']
        city = response.meta['city']
        if search_type == "xinfang":
            district_streets = response.xpath("//div[@class='item-list area-bd']")
            districts = district_streets.xpath("./div[@class='filter']/a")
            streets_blocks = district_streets.xpath("./div[@class='filter-sub']")
            for i in range(len(districts)):
                district_name = districts[i].xpath("./text()").extract_first()
                streets = streets_blocks[i].xpath("./a")
                for street in streets:
                    street_name = street.xpath("./text()").extract_first()
                    street_url = street.xpath("./@href").extract_first()
                    yield scrapy.Request(url=street_url, callback=self.parse_list,
                                         meta={'city': city, 'district': district_name, 'street': street_name,
                                               'search_type': search_type})
        if search_type == "ershoufang":
            pass

    def parse_list(self, response):
        search_type = response.meta['search_type']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        if search_type == "xinfang":
            buildings = response.xpath("//div[@class='infos']")
            for building in buildings:
                detail_url = building.xpath("./a[@class='lp-name']/@href").extract_first()
                if not self.redis.sismember(AnjukeSpider.name, detail_url):
                    yield scrapy.Request(url=detail_url, callback=self.parse_detail,
                                         meta={'city': city, 'district': district, 'street': street,
                                               'housing_url': detail_url, 'search_type': search_type})
                pagination = response.xpath("//div[@class='pagination']")
                next_page = pagination.xpath("./a[@class='next-page next-link']")
                if next_page:
                    next_url = next_page[0].xpath("./@href").extract_first()
                    yield scrapy.Request(url=next_url, callback=self.parse_list,
                                         meta={'city': city, 'district': district, 'street': street,
                                               'search_type': search_type})

    def parse_detail(self, response):
        search_type = response.meta['search_type']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        housing_url = response.meta['housing_url']
        if search_type == "xinfang":
            basic_info = response.xpath("//div[@class='basic-info']")
            housing_name = basic_info.xpath("./h1/text()").extract_first()
            housing_alias_raw = basic_info.xpath("./p")
            price = response.xpath("//dd[@class='price']/p")
            undefined_price = price[0].xpath("./i[@class='sp-price other-price']/text()").extract_first()
            zb_price = price[0].xpath("./em/text()").extract_first()
            zb_price_unit = price[0].xpath("./span/text()").extract_first()
            if undefined_price:
                housing_price = undefined_price + " 周边均价" + zb_price + zb_price_unit
            else:
                housing_price = zb_price + " " + zb_price_unit
            if housing_alias_raw:
                housing_alias = housing_alias_raw.xpath("./text()").extract_first().lstrip("别名：")
            else:
                housing_alias = ""
            more_info_url = response.xpath("//div[@class='more-info']/a/@href").extract_first()
            yield scrapy.Request(url=more_info_url, callback=self.parse_detail_info,
                                 meta={'city': city, 'district': district, 'street': street, 'housing_url': housing_url,
                                       'housing_detail_url': more_info_url, 'housing_name': housing_name,
                                       'housing_alias': housing_alias, 'housing_price': housing_price, 'search_type': search_type})

    def parse_detail_info(self, response):
        items = CaijiItem()
        search_type = response.meta['search_type']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        if search_type == "xinfang":
            housing_name = response.meta['housing_name']
            housing_alias = response.meta['housing_alias']
            housing_price = response.meta['housing_price']
            housing_url = response.meta['housing_url']
            housing_detail_url = response.meta['housing_detail_url']
            # print(city, district, street, housing_name, housing_alias, housing_price, housing_url, housing_detail_url)
            info_blocks = response.xpath("//div[@class='can-left']/div[@class='can-item']")
            for cantainer in info_blocks:
                head = cantainer.xpath("./div[@class='can-head']/h4/text()").extract_first()
                if head == "基本信息" or "小区情况":
                    can = cantainer.xpath("./div[@class='can-border']/ul[@class='list']/li")
                    for pairs in can:
                        label = pairs.xpath("./div[@class='name']/text()").extract_first()
                        if label:
                            label = label.strip()
                        value = pairs.xpath("./div[@class='des']/text()").extract_first()
                        if value:
                            value = value.strip()
                        if label == "物业类型":
                            items['property_type'] = value
                        if label == "开发商":
                            value = pairs.xpath("./div[@class='des']/a/text()").extract_first() or value
                            items['developer'] = value
                        if label == "楼盘地址":
                            items['housing_address'] = value
                        if label == "建筑类型":
                            items['building_type'] = value
                        if label == "规划户数":
                            items['house_total'] = value
                        if label == "物业管理费":
                            items['property_fee'] = value
                        if label == "物业公司":
                            value = pairs.xpath("./div[@class='des']/a/text()").extract_first() or value
                            items['property_company'] = value
                        if label == "车位数":
                            items['parking_place'] = value
                        if label == "车位比":
                            items['parking_ratio'] = value
                        if label == "绿化率":
                            items['greening_rate'] = value
                        if label == "产权年限":
                            items['right_years'] = value
                        if label == "容积率":
                            items['capacity_rate'] = value
            items['city'] = city
            items['district'] = district
            items['street'] = street
            items['housing_name'] = housing_name
            items['housing_alias'] = housing_alias
            items['housing_price'] = housing_price
            items['housing_url'] = housing_url
            items['housing_detail_url'] = housing_detail_url
            yield items
