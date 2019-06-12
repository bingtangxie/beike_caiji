# -*- coding: utf-8 -*-
import scrapy


class XiaoquSpider(scrapy.Spider):
    name = 'xiaoqu'
    allowed_domains = ['ke.com']
    start_urls = ['http://www.ke.com/city']

    def __init__(self):
        super().__init__()
        self.unresolved = 0
        self.resolved = 0
        self.header_ex = []
        self.extra = []

    def parse(self, response):
        city_native = response.xpath("//div[@data-action='Uicode=选择城市_国内']")
        provinces = city_native.xpath(".//div[@class='city_province']")
        # print(len(provinces))
        for province in provinces:
            province_name = province.xpath("./div[@class='city_list_tit c_b']/text()")[0].extract().strip()
            # print(province_name)
            cities = province.xpath("./ul/li")
            for city in cities:
                city_name = city.xpath("./a/text()")[0].extract().strip()
                city_url = city.xpath("./a/@href")[0].extract().strip()
                url = "https:" + city_url
                yield scrapy.Request(url=url, callback=self.parse_city, meta={"province": province_name, "city": city_name})

    def parse_city(self, response):
        header = response.xpath("//div[@class='header']/div[@class='wrapper']/div[@class='fr']/div[@class='nav typeUserInfo']")
        loupan = response.xpath("//div[@class='xinfang-nav']/div[@class='wrapper-xinfang']")
        if header:
            lis = header.xpath(".//ul/li")
            lis_total = len(lis)
            for i in range(lis_total - 4):
                header_type = lis[i].xpath("./a/text()").extract()[0].strip()
                if header_type == "小区":
                    xiaoqu_url = lis[i].xpath("./a/@href").extract()[0]
                    yield scrapy.Request(url=xiaoqu_url, callback=self.parse_list, meta={"type": "xiaoqu", "city": response.meta['city']})
                elif header_type == "新房":
                    xinfang_url = lis[i].xpath("./a/@href").extract()[0]
                    yield scrapy.Request(url=xinfang_url, callback=self.parse_list, meta={"type": "xinfang", "city": response.meta['city']})
                else:
                    pass
            self.resolved += 1
            # xiaoqu = header.xpath(".//ul/li")[4].xpath("./a/text()").extract()[0].strip()
            # xinfang = header.xpath(".//ul/li")[1].xpath("./a/text()").extract()[0].strip()
            # if xiaoqu == "小区":
            #     xiaoqu_url = header.xpath(".//ul/li")[4].xpath("./a/@href").extract()[0]
            #     yield scrapy.Request(url=xiaoqu_url, callback=self.parse_list)
            #     self.resolved += 1
            # elif xinfang == "新房":
            #     xinfang_url = header.xpath(".//ul/li")[1].xpath("./a/@href").extract()[0]
            #     yield scrapy.Request(url=xinfang_url, callback=self.parse_list)
            #     self.resolved += 1
            # else:
            #     self.header_ex.append(response)
        elif loupan:
            loupan_uri = loupan.xpath("./div[@class='fl']/ul/li")[1].xpath("./a/@href").extract()[0]
            loupan_url = response.urljoin(loupan_uri)
            yield scrapy.Request(url=loupan_url, callback=self.parse_district, meta={"type": "loupan", "city": response.meta['city']})
            self.resolved += 1
        else:
            self.extra.append(response)

    def parse_district(self, response):
        # print(self.resolved)
        # print(self.header_ex)
        # print(self.extra)
        search_type = response.meta['type']
        if search_type == 'xiaoqu':
            districts = response.xpath("//div[@data-role='ershoufang']/div/a")
            for district in districts:
                district_name = district.xpath("./text()")[0].extract().strip()
                district_uri = district.xpath("./@href")[0].extract().strip()
                district_url = response.urljoin(district_uri)
                yield scrapy.Request(url=district_url, callback=self.parse_street, meta={"type": response.meta['type'], "city": response.meta['city'], "district": district_name})
        else:
            pass

    def parse_street(self, response):
        search_type = response.meta['type']
        if search_type == "xiaoqu":
            streets_div = response.xpath("//div[@data-role='ershoufang']/div")
            if len(streets_div) != 2:
                # print(response)
                yield scrapy.Request(url=response.url, callback=self.parse_list, meta={"type": response.meta['type'], "city": response.meta['city'], "district": response.meta['district']})
            # else:
            #     for street in streets_div[1].xpath("./a"):
            #         street_name = street.xpath("./text()").extract()[0].strip()
            #         street_uri = street.xpath("./@href").extract()[0].strip()
            #         street_url = response.urljoin(street_uri)
            #         yield scrapy.Request(url=street_url, callback=self.parse_list)
        else:
            pass

    def parse_list(self, response):
        housing_list = response.xpath("//div[@data-component='list']/ul/li")
        print(response)
        print(response.url)
        print(response._url)
        print(response._get_url())
        print(response.request)
        print(housing_list)
        # print(len(housing_list))
        # print(response.meta)
        # if response.meta['flag'] == "1":
        #     print("street")
        # else:
        #     print("no street")

