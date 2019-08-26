#使用流程
新增存檔資料夾test，與test_key資料夾，放key.csv(紀錄flightaware可獲取的callsign)與key_nonavaiable.csv(紀錄flightaware無法取得的callsign)

使用新增表

使用更新表

使用搜尋表

每日固定跑一次更新表

每日固定跑幾次更新表(透過讀取公開資料如opensky獲取不明callsign)

形成一個固定循環，累積成龐大資料。

註:**時區**處理不夠細緻，未必能獲取正確時間

## 參數
long_lat_path: 飛機經緯度資料表位置(飛機相關資料夾中的airport_long_lat1)

save_path(可參考test資料夾):存取檔案資料夾位置

save_keypath(可參考test_key資料夾):存取過去callsign能在flightaware獲得與否


## Airport_longlat

找尋機場經緯度相關資訊時使用，根據你輸入的機場callsign代碼，

自動去Our airport爬取相關資訊，回傳為4個欄位，你輸入的ICAO代碼、IATA代碼、經度、緯度，

如果找不到的話，會是完全空值(NULL)。

範例如附圖(一次可大量搜尋):

![](https://i.imgur.com/ZMyV5p3.png)

![](https://i.imgur.com/ZnewofE.png)


## opensky_crawl_flghtdata

自動去opensky抓那時間區段的飛機資料，自動存檔到規定的位置，

資料包含航班的icao24、callsign資料，與起迄機場的ICAO代碼、IATA代碼與機場中(英)名稱，

與起迄預計時間等資訊。

Crawler_aircraft_fun=function(path_save,Database_Iata_to_Icao,Database_aircraft,
                              Time_start,Time_end)

path_save:檔案存檔的位置

Database_Iata_to_Icao:附檔icao1.csv，功能為ICAO與IATA的轉換，和代表的中(英)機場名稱

Database_aircraft:opensky官網download可下載的資料，內含飛機詳細資訊，機型，機種......等

Time_Start:起始時間(**需使用Timestamp格式**)

Time_end:結束時間(**需使用Timestamp格式**)

**注意****:起始到時間結束不能超過兩小時!!且此方法無法查到最新的航班資訊，大約能存取兩天前的資料。

![](https://i.imgur.com/XQhEGhO.png)

![](https://i.imgur.com/VscR4qZ.png)

## 五種形式(更新表、新增表、即時搜尋不新增、搜尋和快速更新版、完整更新版)
目標:找出飛機的起迄地
輸入:callsign,當下的經緯度

#### 整體流程如下圖:

![](https://i.imgur.com/KiQJ5iT.png)

五種形式說明如下:
Type 1 :更新資料表程式 

Type 2 :新增資料表程式 

Type 3 :運用現有資料表單純回傳起迄地程式 

Type 4 :回傳起迄地，如果沒有此callsign的資料表，會自動去flightaware爬取，但在過去紀錄中，無法爬取的話，就不會再去flightaware爬取，消耗時間
 
Type 5 :回傳起迄地，如果沒有此callsign，就會自動去flightaware爬取資料，無論有沒有出現在過去紀錄中，會重複爬取資料，以免flightaware資料庫有更新。

#### 整體結果如下圖:

輸入資料(目前是用opensky全部資料，實際只使用到上述三個資訊)

![](https://i.imgur.com/mksNaSg.png)

輸出結果:

![](https://i.imgur.com/cTde3In.png)

(展示方便使用data.frame展示，實際回傳為json格式)
