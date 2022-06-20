一、研究目標 : 找出在Fomo前一日哪些群體的參與(買進)，能有效預測Fomo當日發生。<br>

------
二、資料抓取操作與使用(data crawler) :<br>
&ensp;&ensp;&ensp;&ensp;(一)、準備檔案(ex. coolcatsnft_補跑清單0513.xlsx)，可以依目標選擇放入錢包地址or專案契約地址，不能混放<br>
&ensp;&ensp;&ensp;&ensp;(二)、填入api_key<br>
&ensp;&ensp;&ensp;&ensp;(三)、填入產出檔案名稱(nft_name)<br>
&ensp;&ensp;&ensp;&ensp;(四)、檢查events_api變數，若要抓錢包地址需使用account_address參數，專案契約地址使用asset_contract_address<br>
&ensp;&ensp;&ensp;&ensp;(五)、填入event_type，設定要抓取的事件，抓取成交交易單使用"successful"。<br>
&ensp;&ensp;&ensp;&ensp;(六)、填入divide、range_s、range_e。<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;1. divide : 要用多少筆數來切總列數(檔案)、range_s : 執行首序列號、range_e : 執行末序列號<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;2. 切成幾等分就會調用幾隻api_key來平行執行，目前最大三隻api key，若要新增需要多新增api_key變數<br>
&ensp;&ensp;&ensp;&ensp;(七)、在桌面創立一個空的資料夾命名為opensea_wallet，檔案都會在裡面產生。<br>

------
三、資料欄位說明:<br>
&ensp;&ensp;&ensp;&ensp;(一)、以抓取錢包地址、成交交易單事件的產出檔案為例<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;1. event_timestamp :  是成交的時間，和Etherscan Transaction 鏈上時間一致<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;2. event_type : 事件類別(os api 參數)<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;3. token_id : 不是所有NFT項目的token_id都會跟發行總數對應去整齊命名<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;4. num_sales : 此筆token被轉手了幾次，隨著時間累計被交易了幾次，代表token的熱門程度<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;5. listing_time : 發生交易時(成交)的最後(新)掛單時間<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;6. token_owner_address : 當前token的擁有者，每一次調用API都會實時更新<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;7. token_seller_address : 此筆交易token的賣方錢包地址<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;8. deal_price : 成交價格 (不同幣別單位不一樣)<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;9. payment_token_symbol : deal_price 對應的幣別<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;10. payment_token_decimals : 10的幾次方位 (用來除deal_price的)<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;11. payment_token_usdprice : 當時幣別換算約當美金價格(粗略不精確)<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;12. quantity : 此筆交易單token成交數量。(>1代表bundle)<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;13. asset_bundle : bundle銷售的資訊，此欄位保留opensea api 原始json欄位，沒有特別處理。<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;此欄位也是造成data crawler產生的檔案，會需要修復的原因，因含有特殊字符。<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;14. auction_type : 此筆交易的賣家拍賣方式 ex.英式拍、荷蘭拍<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;15. bid_amount : bid價格<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;16. winner_account_address : token買家錢包地址<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;17. collection_slug : 項目名稱<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;18. contract_address : 若token在opensea鑄造會記錄其opensea交易所的地址，一般完整的項目發售都會有自己的合約地址。<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;19. wallet_address_input : 執行data crawler.py 的準備檔案裡的地址資料<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;20. pages : 此筆地址在opensea api裡面的頁數，一頁20筆<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;21. msg : SOMTHING WRONG、Fail-no asset_events代表這筆 wallet_address_input 沒有執行抓完，<br>
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;應整個檔案的此筆wallet_address_input都刪除。<br>

------
四、特徵工程
        
