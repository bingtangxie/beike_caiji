# -*- coding: utf-8 -*-
import scrapy
from caiji.items import CaijiItem
import redis
import pymongo
import re
from scrapy.conf import settings


class FangtianxiaSpider(scrapy.Spider):
    name = 'fangtianxia'
    allowed_domains = ['fang.com']
    start_urls = ['https://www.fang.com/SoufunFamily.htm']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
        self.sum = 0

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
        content = response.xpath("//table[@id='senfe']/tr")
        province = None
        for item in content:
            ss = "".join(item.xpath("./td")[1].xpath("./strong/text()").extract())
            cities = item.xpath("./td")[2].xpath("./a")
            if ss:
                if ss == "直辖市":
                    province = ""
                elif ss == '\xa0':
                    pass
                else:
                    province = ss
            else:
                pass
            for city in cities:
                city_name = city.xpath("./text()").extract()[0]
                city_url = city.xpath("./@href").extract()[0]
                if province != "其它":
                    yield scrapy.Request(url=city_url, callback=self.parse_city,
                                         meta={'province': province, 'city': city_name})

    def parse_city(self, response):
        xinfang = response.xpath("//div[@track-id='newhouse']/div[@class='s4Box']/a")
        ershoufang = response.xpath("//a[@id='dsy_D01_24']")
        province = response.meta['province']
        city = response.meta['city']
        if xinfang:
            link_url = xinfang.xpath('./@href').extract()[0]
            search_type = 'xinfang'
            # yield scrapy.Request(url=link_url, callback=self.parse_district, meta={'province': province, 'city': city, 'search_type': search_type})
        if ershoufang:
            link_url = ershoufang.xpath('./@href').extract()[0]
            search_type = 'ershoufang'
            yield scrapy.Request(url=link_url, callback=self.parse_district,
                                 meta={'province': province, 'city': city, 'search_type': search_type})

    def parse_district(self, response):
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        if search_type == 'xinfang':
            districts = response.xpath("//dd[@id='sjina_D03_05']/ul/li[@id='quyu_name']/a")
            house_total = response.xpath("//li[@id='sjina_C01_30']/a/span/text()").extract_first()
            city_house_total = re.search("\((\d+)\)", house_total).group(1)
            # self.sum = self.sum + int(city_house_total)
            # print(province, city, city_house_total, self.sum)
            if districts:
                for district in districts:
                    if district.xpath("./@style").extract():
                        pass
                    elif district.xpath("./@href").extract()[0] == '#no':
                        pass
                    else:
                        link_uri = district.xpath("./@href").extract()[0]
                        district_name = district.xpath("./text()").extract()[0]
                        link_url = response.urljoin(link_uri)
                        yield scrapy.Request(url=link_url, callback=self.parse_street,
                                             meta={'province': province, 'city': city, 'district': district_name,
                                                   'search_type': search_type})
            else:
                print("# 可能出现验证码")
                pass
                # 可能出现验证码
        if search_type == "ershoufang":
            districts = response.xpath("//div[@class='qxName']/a")
            if districts:
                for district in districts:
                    district_name = district.xpath("./text()").extract_first()
                    if district_name == "不限":
                        pass
                    else:
                        district_uri = district.xpath("./@href").extract_first()
                        district_url = response.urljoin(district_uri)
                        yield scrapy.Request(url=district_url, callback=self.parse_street,
                                             meta={'province': province, 'city': city, 'district': district_name,
                                                   'search_type': search_type})
            else:
                print("# 可能出现验证码")
                pass
                # 可能出现验证码

    def parse_street(self, response):
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        if search_type == 'xinfang':
            streets = response.xpath("//ol[@style='overflow:hidden;']/li/a")
            if streets:
                for street in streets:
                    street_name = street.xpath("./text()").extract()[0]
                    street_uri = street.xpath("./@href").extract()[0]
                    street_url = response.urljoin(street_uri)
                    yield scrapy.Request(url=street_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street_name, 'search_type': search_type})
            else:
                pass
        if search_type == "ershoufang":
            streets = response.xpath("//div[@class='shangQuan']/p[@id='shangQuancontain']/a")
            if streets:
                for street in streets:
                    street_name = street.xpath("./text()").extract_first()
                    if street_name == "不限":
                        pass
                    else:
                        street_uri = street.xpath("./@href").extract_first()
                        street_url = response.urljoin(street_uri)
                        yield scrapy.Request(url=street_url, callback=self.parse_list,
                                             meta={'province': province, 'city': city, 'district': district,
                                                   'street': street_name, 'search_type': search_type})
            else:
                pass

    def parse_list(self, response):
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        if search_type == 'xinfang':
            housing_list = response.xpath("//div[@class='nlcd_name']/a")
            if housing_list:
                for housing in housing_list:
                    housing_url = "https:" + housing.xpath("./@href").extract_first()
                    if housing_url:
                        if not self.redis.sismember(FangtianxiaSpider.name, housing_url):
                            yield scrapy.Request(url=housing_url, callback=self.parse_detail,
                                                 meta={'province': province, 'city': city, 'district': district,
                                                       'street': street, 'housing_url': housing_url,
                                                       'search_type': search_type})
                pagination = response.xpath("//div[@class='page']/ul/li[@class='fr']/a[@class='next']")
                if pagination:
                    next_page_uri = pagination[0].xpath("./@href").extract_first()
                    next_page_url = response.urljoin(next_page_uri)
                    yield scrapy.Request(url=next_page_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'search_type': search_type})
                else:
                    pass
            else:
                # 列表页面为空
                pass
        if search_type == "ershoufang":
            housing_list = response.xpath("//div[@class='houseList']/div[@class='list rel mousediv']")
            if housing_list:
                for housing in housing_list:
                    housing_url_raw = housing.xpath(".//a[@class='plotTit']/@href").extract_first()
                    housing_name = housing.xpath(".//a[@class='plotTit']/text()").extract_first()
                    if housing_url_raw[0:2] == "//":
                        housing_url = "https:" + housing_url_raw
                    else:
                        housing_url = ""  # 没有详细信息就过滤掉了， 类似https://nb.esf.fang.com/house-xm2011122880/
                    undefined_price = housing.xpath("./div[@class='listRiconwrap']/p[@class='not_data']")
                    if undefined_price:
                        housing_price = undefined_price.xpath("./text()").extract_first()
                    else:
                        housing_price = housing.xpath("./div[@class='listRiconwrap']/p[@class='priceAverage']/span")[
                                            0].xpath("./text()").extract_first() + "元/㎡"
                    if housing_url:
                        if not self.redis.sismember(FangtianxiaSpider.name, housing_url):
                            yield scrapy.Request(url=housing_url, callback=self.parse_detail,
                                                 meta={'province': province, 'city': city, 'district': district,
                                                       'street': street, 'housing_name': housing_name,
                                                       'housing_url': housing_url, 'housing_price': housing_price,
                                                       'search_type': search_type})
                pagination = response.xpath("//a[@id='PageControl1_hlk_next']")
                if pagination:
                    next_uri = pagination[0].xpath("./@href").extract_first()
                    next_url = response.urljoin(next_uri)
                    yield scrapy.Request(url=next_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'search_type': search_type})
                else:
                    pass
            else:
                pass

    def parse_detail(self, response):
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        housing_url = response.meta['housing_url']
        if search_type == 'xinfang':
            housing_detail = response.xpath("//div[@class='fl more']/p/a")
            if housing_detail:
                housing_detail_url = "https:" + housing_detail[0].xpath("./@href").extract()[0]
                yield scrapy.Request(url=housing_detail_url, callback=self.parse_detail_info,
                                     meta={'province': province, 'city': city, 'district': district, 'street': street,
                                           'housing_url': housing_url, 'housing_detail_url': housing_detail_url,
                                           'search_type': search_type})
            else:
                pass
        if search_type == "ershoufang":
            housing_name = response.meta['housing_name']
            housing_price = response.meta['housing_price']
            housing_detail = response.xpath("//li[@id='kesfxqxq_A01_03_01']/a")
            housing_detail1 = response.xpath("//div[@class='snav_sq']/ul/li")
            if housing_detail:
                flag = 0
                housing_detail_url = "https:" + housing_detail.xpath("./@href").extract_first()
                yield scrapy.Request(url=housing_detail_url, callback=self.parse_detail_info,
                                     meta={'province': province, 'city': city, 'district': district, 'street': street,
                                           'housing_url': housing_url, 'housing_detail_url': housing_detail_url,
                                           'housing_name': housing_name, 'housing_price': housing_price,
                                           'search_type': search_type, 'flag': flag})

            elif housing_detail1:
                flag = 1
                for li in housing_detail1:
                    text = li.xpath("./a/text()").extract_first()
                    if text == "楼盘详情":
                        housing_detail_url1 = "https:" + li.xpath("./a/@href").extract_first()
                        yield scrapy.Request(url=housing_detail_url1, callback=self.parse_detail_info,
                                             meta={'province': province, 'city': city, 'district': district,
                                                   'street': street,
                                                   'housing_url': housing_url,
                                                   'housing_detail_url': housing_detail_url1,
                                                   'housing_name': housing_name, 'housing_price': housing_price,
                                                   'search_type': search_type, 'flag': flag})
            else:
                pass

    def parse_detail_info(self, response):
        items = CaijiItem()
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        housing_url = response.meta['housing_url']
        housing_detail_url = response.meta['housing_detail_url']
        if search_type == 'xinfang':
            housing_name = response.xpath("//div[@class='lpbt']/h1/a/text()").extract_first()
            housing_alias = response.xpath("//div[@class='lpbt']/span[@class='h1_label']/text()").extract_first()
            housing_base_info = response.xpath("//ul[@class='list clearfix']/li")[0]
            housing_base_info1 = response.xpath("//ul[@class='list clearfix']/li")[1].xpath("./div/li")
            housing_zbss_info = response.xpath("//ul[@class='sheshi_zb']/li")
            housing_plan_info = response.xpath("//ul[@class='clearfix list']/li")
            items['housing_name'] = housing_name
            items['housing_alias'] = housing_alias.strip("别名：") if housing_alias else ""
            items['housing_price'] = response.xpath("//div[@class='main-info-price']/em/text()").extract_first().strip()
            items['property_type'] = housing_base_info.xpath(
                "./div[@class='list-right']/text()").extract_first().strip()
            for base_info in housing_base_info1:
                info_left = "".join(base_info.xpath("./div[@class='list-left']/text()").extract()).strip("：")
                if info_left == "开发 商":
                    info_right = base_info.xpath("./div[@class='list-right-text']/text()").extract_first() or base_info.xpath("./div[@class='list-right-text']/a")[0].xpath(
                        "./text()").extract_first()
                    items['developer'] = info_right
                if info_left == "楼盘地址":
                    info_right = base_info.xpath("./div[@class='list-right-text']/text()").extract_first()
                    items['housing_address'] = info_right
                if info_left == "建筑类别":
                    info_right = base_info.xpath(
                        "./div[@class='list-right']/span/text()").extract_first() or base_info.xpath(
                        "./div[@class='list-right']").extract_first()
                    items['building_type'] = info_right.strip()
                if info_left == "产权年限":
                    info_right = base_info.xpath("./div[@class='list-right']/div/p")
                    right = ""
                    for aa in info_right:
                        right = right + aa.xpath("./text()").extract_first() + " "
                    items['right_years'] = right
            items['education_facility'] = ""
            for zbss in housing_zbss_info:
                info_left = zbss.xpath("./span/text()").extract_first()
                info_right = zbss.xpath("./text()").extract_first()
                if info_left == "交通":
                    items['traffic'] = info_right
                if info_left == "综合商场":
                    items['shopping'] = info_right
                if info_left == "医院":
                    items['hospitail'] = info_right
                if info_left == "邮政":
                    items['postoffice'] = info_right
                if info_left == "银行":
                    items['bank'] = info_right
                if info_left == "小区内部配套":
                    items['internal_suite'] = info_right
                if info_left == "幼儿园":
                    if items['education_facility']:
                        items['education_facility'] = items['education_facility'] + " " + info_right
                    else:
                        items['education_facility'] = info_right
                if info_left == "中小学":
                    if items['education_facility']:
                        items['education_facility'] = items['education_facility'] + " " + info_right
                    else:
                        items['education_facility'] = info_right
                if info_left == "大学":
                    if items['education_facility']:
                        items['education_facility'] = items['education_facility'] + " " + info_right
                    else:
                        items['education_facility'] = info_right
                if info_left == "其他":
                    items['other'] = info_right
            for plan_info in housing_plan_info:
                info_left = "".join(plan_info.xpath("./div[@class='list-left']/text()").extract()).strip("：")
                info_right = "".join(plan_info.xpath("./div[@class='list-right']/text()").extract())
                if info_left == "占地面积":
                    items['area'] = info_right
                if info_left == "建筑面积":
                    items['building_area'] = info_right
                if info_left == "容积率":
                    items['capacity_rate'] = info_right
                if info_left == "绿化率":
                    items['greening_rate'] = info_right
                if info_left == "停车位":
                    items['parking_place'] = info_right
                if info_left == "楼栋总数":
                    items['building_total'] = info_right
                if info_left == "总户数":
                    items['house_total'] = info_right
                if info_left == "物业公司":
                    info_right = "".join(plan_info.xpath("./div[@class='list-right']/a/text()").extract()) or "".join(
                        plan_info.xpath("./div[@class='list-right']/text()").extract())
                    items['property_company'] = info_right
                if info_left == "物业费":
                    items['property_fee'] = info_right
        if search_type == "ershoufang":
            housing_name = response.meta['housing_name']
            housing_price = response.meta['housing_price']
            flag = response.meta['flag']
            if flag == 0:
                info_blocks = response.xpath("//div[@class='box']")
                for information in info_blocks:
                    header = information.xpath("./div[@class='box_tit']")
                    if header:
                        title = header[0].xpath("./h3/text()").extract_first().strip()
                        if title == "基本信息":
                            base_blocks = information.xpath("./div/dl/dd")
                            for pairs in base_blocks:
                                label = pairs.xpath("./strong/text()").extract_first().strip("：")
                                value = pairs.xpath("./text()").extract_first()
                                if label == "小区地址":
                                    items['housing_address'] = value
                                if label == "所属区域":
                                    items['business_circle'] = value
                                if label == "邮    编":
                                    items['postcode'] = value
                                if label == "产权描述":
                                    items['right_years'] = value
                                if label == "物业类别":
                                    items['property_type'] = value
                                if label == "开 发 商":
                                    items['developer'] = value
                                if label == "建筑类型":
                                    items['building_type'] = value
                                if label == "建筑面积":
                                    items['building_area'] = value
                                if label == "占地面积":
                                    items['area'] = value
                                if label == "房屋总数":
                                    items['house_total'] = value
                                if label == "楼栋总数":
                                    items["building_total"] = value
                                if label == "物业公司":
                                    items['property_company'] = value
                                if label == "绿 化 率":
                                    items['greening_rate'] = value
                                if label == "容 积 率":
                                    items['capacity_rate'] = value
                                if label == "物 业 费":
                                    items['property_fee'] = value
                        if title == "交通状况":
                            traffic_block = information.xpath("./div/dl/dt")
                            items['traffic'] = ""
                            for pairs in traffic_block:
                                key_value = pairs.xpath("./text()").extract_first()
                                if items['traffic']:
                                    items['traffic'] = items['traffic'] + " " + key_value
                                else:
                                    items['traffic'] = key_value
                        if title == "周边信息":
                            zb_block = information.xpath("./div/dl/dt")
                            for pairs in zb_block:
                                key_value = pairs.xpath("./text()").extract_first().split("：")
                                if len(key_value) == 2:
                                    label = key_value[0]
                                    value = key_value[1]
                                    if label == "商场":
                                        items['shopping'] = value
                                    if label == "医院":
                                        items['hospitail'] = value
                                    if label == "邮局":
                                        items['postoffice'] = value
                                    if label == "银行":
                                        items['bank'] = value
                                    if label == "小区内部配套":
                                        items['internal_suite'] = value
                                    if label == "其他":
                                        items['other'] = value
                                else:
                                    items['shopping'] = ""
                                    items['hospitail'] = ""
                                    items['postoffice'] = ""
                                    items['bank'] = ""
                                    items['internal_suite'] = ""
                                    items['other'] = ""
                        if title == "配套设施":
                            facility_block = information.xpath('./div/dl/dd')
                            special_block = information.xpath('./div/dl/dt')
                            if facility_block:
                                for pairs in facility_block:
                                    label = pairs.xpath("./strong/text()").extract_first().strip("：")
                                    value = pairs.xpath("./span/text()").extract_first()
                                    if label == "供    水":
                                        items['water_supply'] = value
                                    if label == "供    暖":
                                        items['heating_mode'] = value
                                    if label == "供    电":
                                        items['power_supply'] = value
                                    if label == "燃    气":
                                        items['gas_supply'] = value
                                    if label == "通讯设备":
                                        items['communication_device'] = value
                                    if label == "卫生服务":
                                        value = value or pairs.xpath("./text()").extract_first()
                                        items['health_service'] = value
                                    if label == "小区入口":
                                        value = value or pairs.xpath("./text()").extract_first()
                                        items['commnity_entrance'] = value
                                    if label == "停 车 位":
                                        items['parking_place'] = value
                            if special_block:
                                for pairs in special_block:
                                    label = pairs.xpath("./strong/text()").extract_first().strip("：")
                                    value = pairs.xpath("./text()").extract_first()
                                    if label == "供    水":
                                        items['water_supply'] = value
                                    if label == "供    暖":
                                        items['heating_mode'] = value
                                    if label == "供    电":
                                        items['power_supply'] = value
                                    if label == "燃    气":
                                        items['gas_supply'] = value
                                    if label == "通讯设备":
                                        items['communication_device'] = value
                                    if label == "卫生服务":
                                        value = value or pairs.xpath("./text()").extract_first()
                                        items['health_service'] = value
                                    if label == "小区入口":
                                        value = value or pairs.xpath("./text()").extract_first()
                                        items['commnity_entrance'] = value
                                    if label == "停 车 位":
                                        items['parking_place'] = value
            if flag == 1:
                info_blocks1 = response.xpath("//div[@class='lpblbox1 borderb01 mt10']")
                for information in info_blocks1:
                    header1 = information.xpath("./dl[@class='title02']")
                    if header1:
                        title1 = header1[0].xpath("./dt[@class='name']/text()").extract_first()
                        if title1 == "基本信息":
                            base_blocks1 = info_blocks1.xpath("./dl[@class='xiangqing']/dd")
                            for pairs in base_blocks1:
                                key_value = pairs.xpath("./text()").extract_first().split("：")
                                label = key_value[0]
                                value = key_value[1]
                                if label == "楼盘地址":
                                    value = pairs.xpath("./span/text()").extract_first()
                                    items['housing_address'] = value
                                if label == "所属区域":
                                    items['business_circle'] = value
                                if label == "物业类别":
                                    items['property_type'] = value
                                if label == "建筑类别":
                                    items['building_type'] = value.strip()
                                if label == "开 发 商":
                                    items['developer'] = value
                                if label == "物 业 费":
                                    items['property_fee'] = value
                                if label == "物业公司":
                                    items['property_company'] = value
                                if label == "占地面积":
                                    items['area'] = value
                                if label == "建筑面积":
                                    items['building_area'] = value
                                if label == "停 车 位":
                                    items['parking_place'] = value
                        if title1 == "交通状况":
                            traffic_block1 = information.xpath("./dl[@class='xiangqing']/dt")
                            items['traffic'] = ""
                            for pairs in traffic_block1:
                                key_value = pairs.xpath("./text()").extract_first()
                                if items['traffic']:
                                    items['traffic'] = items['traffic'] + " " + key_value
                                else:
                                    items['traffic'] = key_value
                        if title1 == "周边信息":
                            zb_block1 = information.xpath("./dl[@class='xiangqing']/dt")
                            for pairs in zb_block1:
                                key_value = pairs.xpath("./text()").extract_first().split("：")
                                if len(key_value) == 2:
                                    label = key_value[0]
                                    value = key_value[1]
                                    if label == "商场":
                                        items['shopping'] = value
                                    if label == "银行":
                                        items['bank'] = value
                                    if label == "邮局":
                                        items['postoffice'] = value
                                    if label == "医院":
                                        items['hospitail'] = value
                                    if label == "其他":
                                        items['other'] = value
                                else:
                                    items['shopping'] = ""
                                    items['bank'] = ""
                                    items['postoffice'] = ""
                                    items['hospitail'] = ""
                                    items['other'] = ""
            items['housing_name'] = housing_name
            items['housing_price'] = housing_price
        items['flag'] = search_type
        items['province'] = province
        items['city'] = city
        items['district'] = district
        items['street'] = street
        items['housing_url'] = housing_url
        items['housing_detail_url'] = housing_detail_url
        yield items
