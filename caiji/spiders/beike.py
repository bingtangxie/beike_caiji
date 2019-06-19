# -*- coding: utf-8 -*-
import scrapy
from caiji.items import CaijiItem
import json
import math
import redis
from scrapy.conf import settings


class CaijiSpider(scrapy.Spider):
    name = 'beike'
    allowed_domains = ['ke.com']
    start_urls = ['http://www.ke.com/city']

    def __init__(self):
        super().__init__()
        self.redis = redis.StrictRedis(host=settings['REDIS_HOST'], port=settings['REDIS_PORT'],
                                       db=settings['REDIS_DB'], password=settings['REDIS_PASS'])

    def parse(self, response):
        city_native = response.xpath("//div[@data-action='Uicode=选择城市_国内']")
        provinces = city_native.xpath(".//div[@class='city_province']")
        for province in provinces:
            province_name = province.xpath("./div[@class='city_list_tit c_b']/text()")[0].extract().strip()
            cities = province.xpath("./ul/li")
            print("正在采集{province}的数据".format(province=province_name))
            for city in cities:
                city_name = city.xpath("./a/text()")[0].extract().strip()
                city_url = city.xpath("./a/@href")[0].extract().strip()
                url = "https:" + city_url
                print("正在采集{city}的数据".format(city=city_name))
                yield scrapy.Request(url=url, callback=self.parse_city, meta={"province": province_name, "city": city_name})

    def parse_city(self, response):
        header = response.xpath("//div[@class='header']/div[@class='wrapper']/div[@class='fr']/div[@class='nav typeUserInfo']")
        loupan = response.xpath("//div[@class='xinfang-nav']/div[@class='wrapper-xinfang']")
        province = response.meta['province']
        city = response.meta['city']
        if header:
            lis = header.xpath(".//ul/li")
            lis_total = len(lis)
            for i in range(lis_total - 4):
                header_type = lis[i].xpath("./a/text()").extract()[0].strip()
                if header_type == "小区":
                    xiaoqu_url = lis[i].xpath("./a/@href").extract()[0]
                    yield scrapy.Request(url=xiaoqu_url, callback=self.parse_district, meta={"type": "xiaoqu", "province": province, "city": city})
                elif header_type == "新房":
                    xinfang_url = lis[i].xpath("./a/@href").extract()[0]
                    yield scrapy.Request(url=xinfang_url, callback=self.parse_district, meta={"type": "xinfang", "province": province, "city": city, "xinfang_url": xinfang_url})
                else:
                    pass
        elif loupan:
            loupan_uri = loupan.xpath("./div[@class='fl']/ul/li")[1].xpath("./a/@href").extract()[0]
            loupan_url = response.urljoin(loupan_uri)
            yield scrapy.Request(url=loupan_url, callback=self.parse_district, meta={"type": "loupan", "province": province, "city": city, "xinfang_url": loupan_url})
        else:
            pass

    def parse_district(self, response):
        province = response.meta['province']
        city = response.meta['city']
        search_type = response.meta['type']
        if search_type == 'xiaoqu':
            districts = response.xpath("//div[@data-role='ershoufang']/div/a")
            for district in districts:
                district_name = district.xpath("./text()")[0].extract().strip()
                district_uri = district.xpath("./@href")[0].extract().strip()
                district_url = response.urljoin(district_uri)
                yield scrapy.Request(url=district_url, callback=self.parse_street, meta={"type": search_type, "province": province, "city": city, "district": district_name, "district_url": district_url})
        elif search_type == "xinfang":
            districts = response.xpath("//div[@class='filter-by-area-container']/ul/li")
            xinfang_url = response.meta['xinfang_url']
            for district in districts:
                district_spell = district.xpath("./@data-district-spell")[0].extract()
                district_name = district.xpath("./text()")[0].extract()
                district_url = xinfang_url + "/" + district_spell + "?_t=1"
                streets = []
                districts_streets = response.xpath("//div[@class='bizcircle-container']/ul[@data-district-spell='{}']/li".format(district_spell))
                for street in districts_streets:
                    name = street.xpath("./span/text()")[0].extract()
                    spell = street.xpath("./@data-bizcircle-spell")[0].extract()
                    streets.append({"name": name, "spell": spell})
                if streets:
                    for street in streets:
                        street_name = street['name']
                        street_spell = street['spell']
                        street_url = xinfang_url + "/" + street_spell + "?_t=1"
                        yield scrapy.Request(url=street_url, callback=self.parse_list, meta={"type": search_type, "province": province, "city": city, "district": district_name, "district_url": district_url, "street": street_name, "street_url": street_url, "flag": "street", "xinfang_url": xinfang_url})
                else:
                    pass
                    yield scrapy.Request(url=district_url, callback=self.parse_list, meta={"type": search_type, "province": province, "city": city, "district": district_name, "district_url": district_url, "flag": "district", "xinfang_url": xinfang_url})
        elif search_type == "loupan":
            districts = response.xpath("//div[@class='filter-by-area-container']/ul/li")
            xinfang_url = response.meta['xinfang_url']
            for district in districts:
                district_spell = district.xpath("./@data-district-spell")[0].extract()
                district_name = district.xpath("./text()")[0].extract()
                district_url = xinfang_url + district_spell + "?_t=1"
                yield scrapy.Request(url=district_url, callback=self.parse_list,
                                     meta={"type": search_type, "province": province, "city": city,
                                           "district": district_name, "district_url": district_url,
                                           "xinfang_url": xinfang_url})
        else:
            pass

    def parse_street(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        district_url = response.meta['district_url']
        search_type = response.meta['type']
        if search_type == "xiaoqu":
            streets_div = response.xpath("//div[@data-role='ershoufang']/div")
            if len(streets_div) != 2:
                yield scrapy.Request(url=response.url, callback=self.parse_list, meta={"type": search_type, "province": province, "city": city, "district": district, "district_url": district_url, "street": "", "street_url": ""})
            else:
                for street in streets_div[1].xpath("./a"):
                    street_name = street.xpath("./text()").extract()[0].strip()
                    street_uri = street.xpath("./@href").extract()[0].strip()
                    street_url = response.urljoin(street_uri)
                    yield scrapy.Request(url=street_url, callback=self.parse_list, meta={"type": search_type, "province": province, "city": city, "district": district, "district_url": district_url, "street": street_name, "street_url": street_url})
        else:
            pass

    def parse_list(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        district_url = response.meta['district_url']
        search_type = response.meta['type']
        street = ""
        street_url = ""
        if 'page' in response.meta:
            page = response.meta['page']
        if search_type == 'xiaoqu':
            street = response.meta['street']
            street_url = response.meta['street_url']
            housing_list = response.xpath("//div[@data-component='list']/ul/li")
            for housing in housing_list:
                housing_id = housing.xpath("./@data-housecode")[0].extract()
                housing_url = housing.xpath("./a/@href")[0].extract()
                yield scrapy.Request(url=housing_url, callback=self.parse_detail, meta={"type": search_type, "province": province, "city": city, "district": district, "district_url": district_url, "street": street, "street_url": street_url, "housing_id": housing_id, "housing_url": housing_url})
            pagination = response.xpath("//div[@class='page-box fr']/div/@page-url")
            if pagination:
                page_url = response.urljoin(response.xpath("//div[@class='page-box fr']/div/@page-url")[0].extract())
                page_data = json.loads(response.xpath("//div[@class='page-box fr']/div/@page-data")[0].extract())
                total_page = page_data['totalPage']
                current_page = page_data['curPage']
                if total_page > current_page:
                    next_page = current_page + 1
                    next_url = page_url.format(page=next_page)
                    yield scrapy.Request(url=next_url, callback=self.parse_list, meta={"type": search_type, "province": province, "city": city, "district": district, "district_url": district_url, "street": street, "street_url": street_url, "page": next_page})
        elif search_type == "xinfang":
            xinfang_url = response.meta['xinfang_url']
            flag = response.meta['flag']
            tmp_res = {}
            tmp_res = json.loads(response.text)
            data = tmp_res['data']
            total = data['total']
            pager = data['selected']['pager']
            page = pager['page']
            page_size = pager['pagesize']
            res_list = data['list']

            if res_list:
                if flag == "street":
                    street = response.meta['street']
                for building in res_list:
                    items = {}
                    items['province'] = province
                    items['city'] = city
                    items['district'] = district
                    items['street'] = street
                    items['housing_id'] = building['build_id']
                    items['housing_url'] = response.urljoin(building['url'])
                    items['housing_name'] = building['resblock_name']
                    items['housing_alias'] = building['resblock_alias']
                    items['housing_address'] = building['address']
                    items['housing_price'] = building['average_price']
                    items['business_circle'] = building['bizcircle_name']  # 商圈
                    items['property_type'] = building['house_type']  # 物业类型
                    items['housing_detail_url'] = items['housing_url'] + "xiangqing"
                    yield scrapy.Request(url=items['housing_detail_url'], callback=self.parse_detail, meta={"housing_info": items, "type": search_type})
                if int(total) > page * page_size:
                    total_page = math.ceil(int(total)/page_size)
                    if page < total_page:
                        next_page = page + 1
                        next_url = xinfang_url + "/pg{next_page}".format(next_page=next_page) + "?_t=1"
                        yield scrapy.Request(url=next_url, callback=self.parse_list, meta={"type": search_type, "province": province, "city": city, "district": district, "district_url": district_url, "flag": flag, "street": street, "street_url": street_url, "xinfang_url": xinfang_url})
            else:
                # 整个district或street返回空列表
                pass
        elif search_type == "loupan":
            xinfang_url = response.meta['xinfang_url']
            tmp_res = {}
            tmp_res = json.loads(response.text)
            data = tmp_res['data']
            total = data['total']
            pager = data['selected']['pager']
            page = pager['page']
            page_size = pager['pagesize']
            res_list = data['list']
            if res_list:
                for building in res_list:
                    items = {}
                    items['province'] = province
                    items['city'] = city
                    items['district'] = district
                    items['street'] = street
                    items['housing_id'] = building['build_id']
                    items['housing_url'] = response.urljoin(building['url'])
                    items['housing_name'] = building['resblock_name']
                    items['housing_alias'] = building['resblock_alias']
                    items['housing_address'] = building['address']
                    items['housing_price'] = building['average_price']
                    items['business_circle'] = building['bizcircle_name']  # 商圈
                    items['property_type'] = building['house_type']  # 物业类型
                    items['housing_detail_url'] = items['housing_url'] + "xiangqing"
                    yield scrapy.Request(url=items['housing_detail_url'], callback=self.parse_detail, meta={"housing_info": items, "type": search_type})
                if int(total) > page * page_size:
                    total_page = math.ceil(int(total) / page_size)
                    if page < total_page:
                        next_page = page + 1
                        next_url = xinfang_url + "/pg{next_page}".format(next_page=next_page) + "?_t=1"
                        yield scrapy.Request(url=next_url, callback=self.parse_list,
                                             meta={"type": search_type, "province": province, "city": city,
                                                   "district": district, "district_url": district_url,
                                                   "street": street, "street_url": street_url,
                                                   "xinfang_url": xinfang_url})
            else:
                # 整个district或street返回空列表
                pass
        else:
            pass

    def parse_detail(self, response):
        search_type = response.meta['type']
        items = CaijiItem()
        street = ""
        if search_type == "xiaoqu":
            province = response.meta['province']
            city = response.meta['city']
            district = response.meta['district']
            street = response.meta['street']
            housing_id = response.meta['housing_id']
            housing_url = response.meta['housing_url']
            housing_name = response.xpath("//div[@class='content']/div[@class='title']/h1/text()")[0].extract().strip()
            housing_address = response.xpath("//div[@class='content']/div[@class='title']/div[@class='sub']/text()")[0].extract().strip()
            housing_price_raw = response.xpath("//div[@class='xiaoquPrice clear']/div[@class='fl']/span[@class='xiaoquUnitPrice']/text()") or response.xpath("//div[@class='xiaoquPrice clear']/div[@class='fl']/text()")
            housing_price = housing_price_raw[0].extract().strip()
            hosing_info = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem']")
            for item in hosing_info:
                label = item.xpath("./span[@class='xiaoquInfoLabel']/text()")[0].extract().strip()
                content = item.xpath("./span[@class='xiaoquInfoContent']/text()")[0].extract().strip()
                if label == "建筑类型":
                    items['building_type'] = content
                elif label == "物业费用":
                    items['property_fee'] = content
                elif label == "物业公司":
                    items['property_company'] = content
                elif label == "开发商":
                    items['developer'] = content
                elif label == "楼栋总数":
                    items['building_total'] = content
                elif label == "房屋总数":
                    items['house_total'] = content
                else:
                    pass
            items['province'] = province
            items['city'] = city
            items['district'] = district
            items['street'] = street
            items['housing_id'] = housing_id
            items['housing_url'] = housing_url
            items['housing_name'] = housing_name
            items['housing_address'] = housing_address
            items['housing_price'] = housing_price
            items['house_total'] = ""
            items['greening_rate'] = ""
            items['right_years'] = ""
            items['area'] = ""
            items['capacity_rate'] = ""
            items['water_supply'] = ""
            items['power_supply'] = ""
            items['parking_ratio'] = ""
            items['heating_mode'] = ""
            items['parking_place'] = ""
            items['business_circle'] = ""
            items['housing_detail_url'] = ""
            items['flag'] = search_type
        elif search_type == "xinfang":
            housing_info = response.meta['housing_info']
            details = response.xpath("//ul[@class='x-box']")
            for detail in details:
                for pairs in detail.xpath('./li'):
                    label = pairs.xpath("./span[@class='label']/text()")[0].extract().strip("：")
                    if label == '参考价格':
                        items['housing_price'] = pairs.xpath("./span[@class='label-val']/span/text()")[0].extract().strip()
                    if label == '开发商':
                        items['developer'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '建筑类型':
                        items['building_type'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '绿化率':
                        items['greening_rate'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '占地面积':
                        items['area'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '规划户数':
                        items['house_total'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '产权年限':
                        items['right_years'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '容积率':
                        items['capacity_rate'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '物业公司':
                        items['property_company'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '物业费':
                        items['property_fee'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '供水方式':
                        items['water_supply'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '车位配比':
                        items['parking_ratio'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '供暖方式':
                        items['heating_mode'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '供电方式':
                        items['power_supply'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '车位':
                        items['parking_place'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
            items['province'] = housing_info['province']
            items['city'] = housing_info['city']
            items['district'] = housing_info['district']
            items['street'] = housing_info['street']
            items['housing_id'] = housing_info['housing_id']
            items['housing_url'] = housing_info['housing_url']
            items['housing_name'] = housing_info['housing_name']
            items['housing_address'] = housing_info['housing_address']
            items['business_circle'] = housing_info['business_circle']
            items['property_type'] = housing_info['property_type']
            items['housing_detail_url'] = housing_info['housing_detail_url']
            items['building_total'] = ""
            items['flag'] = search_type
        elif search_type == "loupan":
            housing_info = response.meta['housing_info']
            details = response.xpath("//ul[@class='x-box']")
            for detail in details:
                for pairs in detail.xpath('./li'):
                    label = pairs.xpath("./span[@class='label']/text()")[0].extract().strip("：")
                    if label == '参考价格':
                        items['housing_price'] = pairs.xpath("./span[@class='label-val']/span/text()")[
                            0].extract().strip()
                    if label == '开发商':
                        items['developer'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '建筑类型':
                        items['building_type'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '绿化率':
                        items['greening_rate'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '占地面积':
                        items['area'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '规划户数':
                        items['house_total'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '产权年限':
                        items['right_years'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '容积率':
                        items['capacity_rate'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '物业公司':
                        items['property_company'] = pairs.xpath("./span[@class='label-val']/text()")[
                            0].extract().strip()
                    if label == '物业费':
                        items['property_fee'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '供水方式':
                        items['water_supply'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '车位配比':
                        items['parking_ratio'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '供暖方式':
                        items['heating_mode'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '供电方式':
                        items['power_supply'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
                    if label == '车位':
                        items['parking_place'] = pairs.xpath("./span[@class='label-val']/text()")[0].extract().strip()
            items['province'] = housing_info['province']
            items['city'] = housing_info['city']
            items['district'] = housing_info['district']
            items['street'] = housing_info['street']
            items['housing_id'] = housing_info['housing_id']
            items['housing_url'] = housing_info['housing_url']
            items['housing_name'] = housing_info['housing_name']
            items['housing_address'] = housing_info['housing_address']
            items['business_circle'] = housing_info['business_circle']
            items['property_type'] = housing_info['property_type']
            items['housing_detail_url'] = housing_info['housing_detail_url']
            items['building_total'] = ""
            items['flag'] = search_type
        else:
            pass
        if not self.redis.sismember(CaijiSpider.name, items['housing_id']):
            yield items


