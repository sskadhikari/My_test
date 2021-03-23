import os
from datetime import datetime,timedelta
from time import gmtime, strftime
from google.cloud import bigquery
import pandas as pd
# Big Query Client
client = bigquery.Client()
query = """
    select * from prod_shopify_1.orders WHERE created_at BETWEEN '2021-01-01' AND '2021-01-31';
"""
def checkList (lists) :
    arr = []
    for i in lists:
        if 'list' in str(type(i)):
            arr.append(checkList(i))
        if 'dict' in str(type(i)):
            arr.append(checkDict(i))
        else:
            arr.append(str(i))
    return arr
def checkDict (dicttt):
    my_dict = {}
    for i in dicttt:
        if 'list' in str(type(dicttt[i])):
            my_dict[i] = checkList(dicttt[i])
        if 'dict' in str(type(dicttt[i])):
            my_dict[i] = checkDict(dicttt[i])
        else:
            my_dict[i] = str(dicttt[i])
    return my_dict
query_job = client.query(query)
data = list(query_job.result())
column = ['presentment_currency', 'event_at', 'total_price_usd', 'total_price',
       'line_items', 'processing_method', 'order_number', 'confirmed',
       'total_discounts', 'total_line_items_price', 'order_adjustments',
       'shipping_lines', 'admin_graphql_api_id', 'device_id', 'cancel_reason',
       'currency', 'payment_gateway_names', 'source_identifier', 'id',
       'processed_at', 'referring_site', 'contact_email', 'location_id',
       'fulfillments', 'customer', 'test', 'total_tax', 'payment_details',
       'number', 'email', 'source_name', 'landing_site_ref',
       'shipping_address', 'closed_at', 'discount_applications', 'name',
       'note', 'user_id', 'source_url', 'subtotal_price', 'billing_address',
       'landing_site', 'taxes_included', 'token', 'app_id',
       'total_tip_received', 'browser_ip', 'discount_codes', 'tax_lines',
       'phone', 'note_attributes', 'fulfillment_status', 'order_status_url',
       'client_details', 'buyer_accepts_marketing', 'checkout_token', 'tags',
       'financial_status', 'customer_locale', 'checkout_id', 'total_weight',
       'gateway', 'cart_token', 'cancelled_at', 'refunds', 'created_at',
       'updated_at', 'reference']
datas = []
count = 1
for i in data:
    json_dict = {}
    for j in range(len(column)):
        json_dict[column[j]] = i[j] # presement_currency = column[j]
    json_dict['event_at'] = int(datetime.strptime(json_dict['created_at'].strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S').timestamp())
    json_dict['op_event_type'] = 0
    for i in json_dict:
        if 'list' in str(type(json_dict[i])):
            json_dict[i] = checkList(json_dict[i])
        if 'dict' in str(type(json_dict[i])):
            json_dict[i] = checkDict(json_dict[i])
        else:
            json_dict[i] = str(json_dict[i])
    datas.append(json_dict)
    print(count)
    count += 1
errors = client.insert_rows_json('prod_shopify_2.orders_test_2',datas)
if errors == []: print('inserted')
else: print(errors)