# -*- coding: utf-8 -*-
import scrapy
import json


class TestSpider(scrapy.Spider):
    name = 'test'
    allowed_domains = ['ke.com']
    start_urls = ['https://api.map.baidu.com/?qt=bda&c=244&wd=%E5%9C%B0%E9%93%81%24%24%E6%95%99%E8%82%B2%24%24%E5%8C%BB%E9%99%A2%24%24%E8%B4%AD%E7%89%A9%24%24%E5%85%AC%E5%9B%AD&wdn=5&ar=(13518144.18%2C3300820.17%3B13522144.16%2C3304820.18)&rn=10&l=18&ie=utf-8&oue=1&fromproduct=jsapi&res=api&callback=BMap._rd._cbk63546&ak=dASz7ubuSpHidP1oQWKuAK3q']

    def parse(self, response):
        print(response.text)

