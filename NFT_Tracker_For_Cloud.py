#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#pip install opensea-api
#pip install selenium
#pip install wappdriver
#pip install pywhatkit
#pip install pymongo
#pip install pandas
#pip install requests
#pip install selenium
#!pip install "pymongo[srv]"
import opensea
#from opensea import Asset
#from opensea import Assets
#from opensea import Collection
import pandas as pd
import rpa as r
import requests
from selenium import webdriver
import os
#import pywhatkit
from datetime import datetime
from datetime import timedelta
import time as ti
import matplotlib.pyplot as plt
import numpy as np
import string
import pymongo
from pymongo import MongoClient


# This function returns our desired properties as a list
def asset_properties(ACA, t_id):
    api = Asset(asset_contract_address = ACA,
                token_id = t_id)
    
    # Pull the values and stats
    vals = api.fetch()
    collection_vals = vals["collection"]
    a_stats = collection_vals["stats"]
    a_traits = vals["traits"]
    
    # Pull the traits
    all_traits = []
    num_traits = len(a_traits)

    # Get all traits as a list
    for i in range(0, num_traits):
        all_traits.append([])

    for i in range(0, num_traits):
        temp_trait = a_traits[i]
        tt_type = temp_trait["trait_type"]
        tt_val  = temp_trait["value"]
        tt_count = temp_trait["trait_count"]
        temp_trait_row = [tt_type,
                          tt_val,
                          tt_count]
        all_traits[i] = temp_trait_row
    # Put as list
    
    all_properties = {"asset_name": vals["name"],
                     "a_cont_address": vals["asset_contract"]["address"],
                     "slug": collection_vals['slug'],
                     "avg_price_1d": a_stats["one_day_average_price"],
                     "avg_price_7d": a_stats["seven_day_average_price"],
                     "avg_price_30d": a_stats["thirty_day_average_price"],
                     "a_count": a_stats["count"],
                     "a_prim_url": vals["external_link"],
                     "twitter_acc": collection_vals["twitter_username"],
                     "sell_order": vals["sell_orders"],
                     "traits": all_traits}
    return all_properties

# We put the properties data we received in better format
def prop_dat(our_assets, property_list, full_list_properties):
    for i in range(0, len(our_assets)):
        temp_ACA = our_assets[i][0]
        temp_id = our_assets[i][1]
        temp_ap = asset_properties(ACA = temp_ACA, 
                                   t_id = temp_id)

        for i in range(0, len(property_list)):
            temp_prop = property_list[i]
            temp_val = temp_ap[temp_prop]
            full_list_properties[temp_prop].append(temp_val)

    #return pd.DataFrame(full_list_properties)
    return full_list_properties

# We put the traits data we received in better format, create a superset for traits
def trait_dat(our_assets, traits_list):
    for i in range(0, len(our_assets)):
        temp_ACA = our_assets[i][0]
        temp_id = our_assets[i][1]
        temp_ap = asset_properties(ACA = temp_ACA,
                                       t_id = temp_id)
        temp_traits = temp_ap["traits"]
        temp_name = temp_ap["asset_name"]
        temp_count = temp_ap["a_count"]
        for k in range(0, len(temp_traits)):
            num_trait = temp_traits[k][2]
            rarity = round((num_trait / temp_count)*100, 2)
            temp_traits[k].append(rarity)
            temp_traits[k].append(temp_name)
            traits_list.append(temp_traits[k])
            
    #return pd.DataFrame(traits_list)
    return traits_list

# This function returns the assets of a provided wallet address
def find_assets(owner_address):
    api = Assets()
    val = api.fetch(owner = owner_address)
    num_assets = len(val['assets'])
    assets_list = {
        'collection_name': [],
        'token_id': [],
        'ACA': [],
    }

    for i in range(0, num_assets):
        # Pull necessary information
        temp_asset = val['assets'][i]
        temp_collection = temp_asset['collection']
        temp_collection_name = temp_collection["name"]
        temp_t_id = temp_asset['token_id']
        temp_ACA = temp_asset['asset_contract']['address']
        # Write into assets list directory
        assets_list['collection_name'].append(temp_collection_name)
        assets_list['token_id'].append(temp_t_id)
        assets_list['ACA'].append(temp_ACA)

    return assets_list

# This function pulls the asset data of a user
def create_asset_list(owner_address):
    our_assets_list = find_assets(owner_address)
    # ACA
    asset_ACA = our_assets_list['ACA']
    # Token ID
    asset_t_id = our_assets_list['token_id']
    len_ACA = len(asset_ACA)
    our_assets = []
    for k in range(0, len_ACA):
        temp_list = []
        temp_list.append(asset_ACA[k])
        temp_list.append(asset_t_id[k])
        our_assets.append(temp_list)
    return our_assets

# This function returns the floor prices which are normally not returned
def add_floor_prices(our_props):
    floor_prices = []

    for i in range(0,len(our_props['slug'])):
        temp_slug = our_props['slug'][i]
        api = Collection(collection_slug=temp_slug)
        temp_floor_price = api.fetch()['collection']['stats']['floor_price']
        floor_prices.append(temp_floor_price)
    
    our_props['floor_price'] = floor_prices
    now = datetime.now()
    our_props["date_and_time"] = [now.strftime("%m/%d/%Y, %H:%M:%S")] * len(our_props['floor_price'])
    return our_props

# Removing duplicates from a dictionary
def removing_dups(input_raw):
    seen = []
    for k,val in input_raw.items():
        if val in seen:
            del input_raw[k]
        else:
            seen.append(val)
    return input_raw

# traits list turned into dictionary
def trait_dict(our_traits):
    traits_dict = {
        "trait_category": [],
        "our_property": [],
        "num_available": [],
        "rarity": [],
        "asset_name": []
    }
    for z in range(0, len(our_traits)):
        trait_card = our_traits[z]
        traits_dict['trait_category'].append(trait_card[0])
        traits_dict['our_property'].append(trait_card[1])
        traits_dict['num_available'].append(trait_card[2])
        traits_dict['rarity'].append(trait_card[3])
        traits_dict['asset_name'].append(trait_card[4])
    return traits_dict
      

# Write Aggregation Tables Here

# Varun's Address
#owner_address = "0x4BCBBF03455C000A3CC10CA801866Cf624426AdA"

# Binayak's Address
#owner_address = "0x00063ddb30be7bc2292583d5f143e9d6e6228440"

address_book = {
    "API_file_address": ["NFT_Tracking.csv"],
    "Varun_Sehgal": ["0x4BCBBF03455C000A3CC10CA801866Cf624426AdA"],
    "Binayak_Pande": ["0x00063ddb30be7bc2292583d5f143e9d6e6228440"]
}

property_list = ["asset_name",
                 "a_cont_address",
                 "slug",
                 "avg_price_1d",
                 "avg_price_7d",
                 "avg_price_30d",
                 "a_count",
                 "a_prim_url",
                 "twitter_acc",
                 "sell_order"]
full_list_properties = {
    "asset_name": [],
    "a_cont_address": [],
    "slug": [],
    "avg_price_1d": [],
    "avg_price_7d": [],
    "avg_price_30d": [],
    "a_count": [],
    "a_prim_url": [],
    "twitter_acc": [],
    "sell_order": []
    }
traits_list = []




owner_address = address_book['Varun_Sehgal']
owner_address = owner_address[0]
our_assets = create_asset_list(owner_address)
our_props = prop_dat(our_assets, property_list, full_list_properties)
our_props = add_floor_prices(our_props)
our_props = removing_dups(our_props)
our_traits = trait_dat(our_assets, traits_list)
our_traits = trait_dict(our_traits)
our_traits = removing_dups(our_traits)
client = pymongo.MongoClient("mongodb+srv://cemcivelek:SerefsizJoe123@nftcluster.1kabs.mongodb.net/NftPerformance?retryWrites=true&w=majority")
db = client.NftPerformance
nperf = db.NftPerformance
result = nperf.insert_one(our_props)
print(f"Props ID: {result}")
traits = db.NftTraits
result = traits.insert_one(our_traits)
print(f"Traits ID: {result}")

