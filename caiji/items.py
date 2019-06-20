# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CaijiItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    province = scrapy.Field()
    city = scrapy.Field()
    district = scrapy.Field()
    street = scrapy.Field()
    housing_id = scrapy.Field()
    housing_url = scrapy.Field()
    housing_name = scrapy.Field()
    housing_alias = scrapy.Field()
    housing_address = scrapy.Field()
    housing_price = scrapy.Field()
    building_type = scrapy.Field()
    property_fee = scrapy.Field()
    property_company = scrapy.Field()
    property_type = scrapy.Field()
    developer = scrapy.Field()
    building_total = scrapy.Field()
    house_total = scrapy.Field()
    greening_rate = scrapy.Field()
    right_years = scrapy.Field()
    area = scrapy.Field()
    capacity_rate = scrapy.Field()
    water_supply = scrapy.Field()
    power_supply = scrapy.Field()
    parking_ratio = scrapy.Field()
    heating_mode = scrapy.Field()
    parking_place = scrapy.Field()
    business_circle = scrapy.Field()
    housing_detail_url = scrapy.Field()
    flag = scrapy.Field()

    traffic = scrapy.Field()
    shopping = scrapy.Field()
    hospitail = scrapy.Field()
    postoffice = scrapy.Field()
    bank = scrapy.Field()
    internal_suite = scrapy.Field()
    other = scrapy.Field()
    building_area = scrapy.Field()
    postcode = scrapy.Field()
    gas_supply = scrapy.Field()
    communication_device = scrapy.Field()
    health_service = scrapy.Field()
    commnity_entrance = scrapy.Field()
    education_facility = scrapy.Field()

    built_year = scrapy.Field()
