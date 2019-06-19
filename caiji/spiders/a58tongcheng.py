# -*- coding: utf-8 -*-
import scrapy
import re
import json
from caiji.items import CaijiItem


class A58tongchengSpider(scrapy.Spider):
    name = '58tongcheng'
    allowed_domains = ['58.com']
    start_urls = ['https://www.58.com/changecity.html']

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
