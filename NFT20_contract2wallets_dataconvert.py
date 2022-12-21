# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:39:59 2022

@author: Fred
"""

import numpy as np
import pandas as pd
import os
import time

#'NFT20_success改版.feather' 同 "NFT20_successful_events_new_有winneraddress.xlsx"
data_dir = os.path.join(os.getcwd(), 'data')
df = os.path.join(data_dir, 'NFT20_success改版.feather')
start_time = time.time()
df_all = pd.read_feather(df)
total_time = time.time() - start_time
print("Total seconds to load:", total_time)

#非bundle情況
df_all = df_all[df_all["quantity"]==1]

#新增買方錢包欄位
df_all["買方錢包"] = df_all["winner_account_address"]
#買方錢包array
buyer_address = df_all["買方錢包"].unique()
#買方錢包賣出token的交易資料
buyer_selling_df = df_all[df_all.token_seller_address.isin(buyer_address)]

#買方錢包買進賣出token的所有交易資料
dfbyer_concat = pd.concat([df_all, buyer_selling_df])
#同時是買方也是賣方的錢包特例，將之刪除。
dfbyer_concat['買方賣方矛盾'] = np.where((dfbyer_concat['token_seller_address'] == dfbyer_concat["買方錢包"]), "Y", "N")
dfbyer_concat = dfbyer_concat[dfbyer_concat['買方賣方矛盾']=="N"]

#去重複
dfbyer_concat = dfbyer_concat.drop_duplicates(subset=['event_timestamp', 'collection_slug', 'token_id', "買方錢包"])
dfbyer_concat_sort = dfbyer_concat.sort_values(by=["買方錢包", 'event_timestamp'],ascending = [True, False])
dfbyer_concat_sort = dfbyer_concat_sort.reset_index(drop = True)

dfbyer_concat_sort= dfbyer_concat_sort.drop(['token_owner_address', 'from_account_address',"starting_price","ending_price","approved_account","asset_bundle", \
                                             "transaction_hash","block_hash","block_number","is_private","duration","created_date","custom_event_name", \
                                                 "dev_fee_payment_event","dev_seller_fee_basis_points","pages","next_param",'買方賣方矛盾'], axis = 1)

#(注意)表格的wallet_address_input 為項目合約地址，錢包地址需以買方錢包為主
dfbyer_concat_sort.to_parquet(os.path.join(data_dir, 'nft20_success_fred.parquet'), compression='lz4')


