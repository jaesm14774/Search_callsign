library(data.table)
library(rvest)
library(dplyr)
library(stringr)
library(tidytext)
library(jiebaR)
library(magrittr)
library(httr)
library(tibble)
library(RSQLite)
library(jsonlite)
library(lubridate)
library(readr)
library(RMySQL)
library(gtools)
library(glue)
library(ropencc)
library(geosphere)
#不要科學記號表示
options(scipen=999)
long_lat_path='C:/Users/jaesm14774/Desktop/飛機相關/airport_long_lat1.csv'
long_lat=fread(input = long_lat_path,integer64 = 'numeric')
setkey(long_lat,'ICAO')
save_path='C:/data/test/'
save_keypath='C:/data/test_key/'
all_file=list.files(path = save_path,pattern = '.csv',full.names =T ,
                    recursive=F)
cc=converter(S2TWP)
timezone_trans=function(x,Date){
  x=gsub(pattern = '\\(\\?\\)',replacement = '',x=x)
  #+07(+1) | CST(-1) QQ~~~
  x=gsub(pattern = '\\s+$',replacement = '',x = x)
  P=grepl(pattern = '\\)$',x = x) %>% which
  if(length(P)>0){
    N=str_extract(string = x[P],pattern = '\\([+-]*\\d+\\)') %>%
      str_match(string = .,pattern = '[+-]*\\d+') %>% as.numeric
    for(k in 1:length(N)){
      if(N[k]>0){
        Date[P[k]]=(Date[P[k]] %>% as_date() +days(N[k])) %>% as.character()
      }else{
        Date[P[k]]=(Date[P[k]] %>% as_date() -days(abs(N[k]))) %>% as.character()
      } 
    }
  }
  x=gsub(pattern = '\\([-+]*\\d+\\)',replacement = '',x=x)
  #處理上午下午
  p2=which(grepl(pattern='上午',x=x) & (str_sub(x,1,2) == 12))
  p=(grepl(pattern = '下午',x = x) & (str_sub(x,1,2) != 12))%>% 
    which
  x=gsub(x = x,pattern = '上午|下午',replacement = '')
  x[p2]=paste0('00',str_sub(string = x[p2],start = 3))
  x[p]=paste0(str_sub(string = x[p],1,2) %>% as.numeric+12,
              str_sub(string = x[p],start = 3))
  x=paste(Date,x,sep=' ')
  deter=str_match(string = x,pattern = '\\s+[A-Z]+.+|\\s+[\\+-][0-9]+.+')
  deter=gsub(pattern = '\\s+',replacement = '',x = deter) %>% .[,1]
  x=gsub(pattern = '\\s+$',replacement = '',x = x)
  x=str_sub(string = x,start = 1,end = -5)
  #AEST|ChST|PGT|CHUT|DDUT|YAPT UTC+10
  p=which(grepl(pattern = 'PGT',x = deter) & 
            (nchar(deter) == 3))
  if(length(p) >0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(10) 
  }
  p=which(grepl(pattern ='DDUT|AEST|ChST|CHUT|YAPT',x = deter) & 
            (nchar(deter) == 4))
  if(length(p) >0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(10) 
  }
  #NZST|ANAST|ANAT|FJT|NRT UTC+12
  p=which(grepl(pattern = 'FJT|NRT',x = deter) & 
            (nchar(deter) == 3))
  if(length(p) >0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(12) 
  }
  p=which(grepl(pattern = 'NZST|ANAT',x = deter) & (nchar(deter) == 4))
  if(length(p) >0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(12) 
  }
  p=which(grepl(pattern = 'ANAST',x = deter) & (nchar(deter) == 5))
  if(length(p) >0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(12) 
  }
  #CST|NST|MYT|HKT|SGT|AWST|BDT|BNT|CHOT|CIT|IRKT|MST|PHT|ULAT|WST|PST|WITA UTC+8
  p=
    which(grepl(pattern = 'CST|NST|MYT|HKT|SGT|BDT|BNT|CIT|MST|PHT|WST|PST',
                x = deter) & (nchar(deter) == 3))
  if(length(p) >0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(8) 
  }
  p=
    which(grepl(pattern = 'AWST|CHOT|IRKT|ULAT|WITA',
                x = deter) & (nchar(deter) == 4))
  if(length(p) >0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(8) 
  }
  #KST|AWDT|CHOST|IRKST|JST|PWT|TLT|ULAST|WIT|YAKT UTC+9
  p=which(grepl(pattern = 'KST|JST|PWT|TLT|WIT',x = deter)&
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(9) 
  }
  p=which(grepl(pattern = 'AWDT|YAKT',x = deter)& 
            (nchar(deter) ==4))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(9) 
  }
  p=which(grepl(pattern='CHOST|IRKST|ULAST',x = deter)& 
            (nchar(deter) ==5))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(9) 
  }
  #ICT|CXT|DAVT|HOVT|KRAT|THA|WIB UTC+7
  p=which(grepl(pattern ='ICT|CXT|THA|WIB',x = deter) &
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(7) 
  }
  p=which(grepl(pattern ='DAVT|HOVT|KRAT',x = deter) &
            (nchar(deter) == 4))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(7) 
  }
  #MSK|EEST|EAT|AST|IDT|TRT UTC+3
  p=which(grepl(pattern = 'MSK|EAT|AST|IDT|TRT',x = deter) &
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(3) 
  }
  p=which(grepl(pattern = 'EEST',x = deter) &
            (nchar(deter) == 4))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(3) 
  }
  #CEST|SAST|WAST|EET UTC+2
  p=which(grepl(pattern = 'CEST|SAST|WAST',x = deter)&
            (nchar(deter) == 4))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(2) 
  }
  p=which(grepl(pattern = 'EET',x = deter)&
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(2) 
  }
  #CET|BST|WAT|WEST UTC+1
  p=which(grepl(pattern = 'CET|BST|WAT',x = deter) &
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(1) 
  }
  p=which(grepl(pattern = 'WEST',x = deter) &
            (nchar(deter) == 4))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(1) 
  }
  #UTC|GMT|WET UTC+0
  p=which(grepl(pattern = 'UTC|GMT|WET',x = deter) &
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(1) 
  }
  #ADT UTC-3
  p=which(grepl(pattern = 'ADT',x = deter) &
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') +hours(3) 
  }
  #EDT UTC-4
  p=which(grepl(pattern = 'EDT',x = deter) &
            (nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') +hours(4) 
  }
  #CDT|EST UTC-5
  p=which(grepl(pattern = 'CDT|EST',x = deter)&(nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') +hours(5) 
  }
  #AKDT UTC-8
  p=which(grepl(pattern = 'AKDT',x = deter)&(nchar(deter) == 4))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') +hours(8) 
  }
  #PDT UTC-7
  p=which(grepl(pattern = 'PDT',x = deter)&(nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') +hours(7) 
  }
  #MDT UTC-6
  p=which(grepl(pattern = 'MDT',x = deter)&(nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') +hours(6) 
  }
  #HST UTC-10
  p=which(grepl(pattern = 'HST',x = deter)&(nchar(deter) == 3))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') +hours(10) 
  }
  #不唯一代碼... IST UTC+5:30
  p=which(grepl(pattern = 'IST',x = deter))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(5)-minutes(30) 
  }
  #ACST UTC +9:30
  p=which(grepl(pattern = 'ACST',x = deter))
  if(length(p)>0){
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(9)-minutes(30) 
  }
  #處理 +7 +8 -1 -3 這種形式
  p=which(grepl(pattern='[-+]\\d+$',x = deter))
  if(length(p)>0){
    temp=str_sub(string = deter[p],start = -3) %>% as.numeric
    x[p]=x[p] %>% as.POSIXct(tz='UTC') -hours(temp)
  }
  return(x)
}

#新增新的callsign紀錄
Add_callsign=function(callsign,callsign_past_record){
  b=callsign_past_record
  if(any(callsign %in% b)){
    stop('包含了已有的callsign代碼，請確認')
  }else print('good')
  B=callsign
  #紀錄沒有歷史資料或無法讀取的callsign
  nonavaiable_key=NULL
  #修改像CAL070成CAL70，0開頭的數字=>去掉0 (flightaware 習慣 跟 opensky不同)
  p=str_extract(string = B,pattern = '\\d{3}') %>%
    str_detect(pattern = '^0') %>% which
  #非數字結尾的callsign不理他
  p_temp=str_detect(string=B,pattern = '[^0-9]$') %>% which
  p=setdiff(p,p_temp)
  b_unrevise=B
  B[p]=
    str_sub(string = B[p],start = 4) %>% as.numeric %>%
    paste0(str_sub(B[p],start = 1,end = 3),.)
  #開始存檔
  for(i in 1:length(B)){
    Sys.setlocale(locale='chinese')
    url=
      GET(url = paste0('https://zh-tw.flightaware.com/live/flight/',B[i],'/history/30'),
          set_cookies(`_ga`='GA1.2.1962256651.1560820244',
                      `__qca`="P0-1803542495-1560820245772",
                      `_gid`="GA1.2.1607624027.1562163473",
                      `__zlcmid`="t6iMHXmujlSvF1",
                      `_fbp`="fb.1.1562287972979.641601648",
                      `update_time`='1562380091',
                      `w_sid`="550f6bb8880950db36f073a1bd9987c8ce53177c7ac31d5e542646330fc82bfc"))
    if(url$status_code !=200){
      nonavaiable_key=c(nonavaiable_key,B[i])
      next()
    }else print('a')
    tab=
      tryCatch(
        {url %>% read_html %>%
            html_table(fill=T) %>% .[[5]]},error=function(e){
              return(0)
            })
    #補救方法(替換等價的callsign，看看是否能得到解套)
    if(class(tab) != 'data.frame'){
      trans=
        read_html(paste0('https://zh-tw.flightaware.com/live/flight/',B[i],'/history/30')) %>%
        html_nodes('.flightPageAdBottom.flightPageNewAdUnit') %>% 
        html_nodes('.flightPageAdUnit') %>%
        html_text(trim=T) %>% as.character() %>%
        str_extract(pattern = 'prefix.{8}') %>%
        str_extract('[A-Z]{2,3}')
      temp_b=str_replace(string = B[i],pattern = '[A-Z]{2,3}',
                         replacement = trans)
      tab=
        tryCatch(
          {read_html(paste0('https://zh-tw.flightaware.com/live/flight/',
                            temp_b,'/history/30')) %>%
              html_table(fill=T) %>% .[[5]]},error=function(e){
                return(0)
              })
      
    }else print('b')
    if(class(tab) != 'data.frame'){
      nonavaiable_key=c(nonavaiable_key,B[i])
      next()
    }else print('c')
    Sys.setlocale(category='LC_ALL')
    p=grepl(pattern = '更多|註冊會員',x = tab[,1]) %>% which
    if(length(p)>0){
      tab=tab[-p,] 
    }else tab=tab
    tab[,3]=run_convert(cc,tab[[3]])
    tab[,4]=run_convert(cc,tab[[4]])
    #刪除已改航的航班(無意義資料)
    p=which(tab[,6] == '' | grepl(pattern = '已取消',x=tab[,7]))
    if(length(p)>0){
      tab=tab[-p,]
    }else tab=tab
    if(str_detect(string = tab[1,1],pattern = 'No History Data')){
      nonavaiable_key=c(nonavaiable_key,B[i])
      next()
    }else print('d')
    temp=tab[,1] %>% str_match('(\\d{4}).+(\\d{2}).+(\\d{2})') %>% 
      .[,2:4,drop=F]
    Date=NULL
    for(j in 1:nrow(temp)){
      Date=c(Date,glue_collapse(temp[j,],sep='-'))
    }
    print('e')
    tab[,5]=
      tryCatch(
        {timezone_trans(x = tab[,5],Date = Date) %>% 
            as.numeric %>% 
            as.POSIXct(origin='1970-01-01') %>% 
            as.character()},
        error=function(e){
          return(e)
        })
    tab[,6]=
      tryCatch(
        {timezone_trans(x = tab[,6],Date = Date) %>% 
            as.numeric %>% as.POSIXct(origin='1970-01-01') %>% 
            as.character()},
        error=function(e) return(e))
    print('f')
    tab=tab[,-1]
    print('g')
    rownames(tab)=1:dim(tab)[1]
    print('h')
    #建立新表
    Sys.setlocale(category='LC_ALL')
    write.csv(paste0(path=save_path,b_unrevise[i],'.csv'),
              x = tab,row.names = F,fileEncoding = 'Big5') 
    print(i)
    Sys.setlocale(category='LC_ALL')
    Sys.sleep(runif(1,4,10))
  }
  #儲存新的callsign到key
  new_key=setdiff(b_unrevise,nonavaiable_key)
  if(length(new_key)>0){
    write_csv(x=data.frame(key=new_key,stringsAsFactors = F),
              append=T,path = paste0(save_keypath,'key.csv'))
  }else print('QQ,no new key is avaiable')
  #儲存不可取得key表或更新
  deter=
    list.files(path=save_keypath,
               pattern='key_nonavaiable.csv',
               full.names = T,
               recursive = F)
  if(length(deter) == 0){
    write_csv(save_keypath,x = data.frame(key=nonavaiable_key)) 
  }else{
    #如果有key突然更新，變可取得的話，更新不可取得的key表
    available_keynow=new_key 
    temp=read.csv(deter,h=T,stringsAsFactors = F) %>% .[,1]
    temp=c(nonavaiable_key,temp) %>% unique
    p=which(temp %in% available_keynow)
    if(length(p)>0){
      temp=temp[-p]
    }else temp=temp
    write_csv(paste0(save_keypath,'key_nonavaiable.csv'),
              x = data.frame(temp)) 
  }
  return(nonavaiable_key)
}
#找尋機場對應經緯度

Search_longlat=function(airport_name){
  airport_name=str_extract(pattern ='\\(.+\\)',string = airport_name) %>%
    gsub(pattern = '\\(|\\)',replacement = '')
  ICAO=str_extract(airport_name,pattern = '\\w{4}')
  get_longlat=long_lat[ICAO] %>% .[,3:4]
  if(dim(get_longlat)[1] == 1){
    get_longlat=glue_collapse(get_longlat[1,],sep = ',')
  }else{
    get_longlat=apply(get_longlat,1,glue_collapse,sep=',')
  }
  return(get_longlat)
}
#找尋點到直線的最短距離
dist_self=function(point,deter){
  d=NULL
  for(i in 1:dim(deter)[1]){
    a1=deter[i,3] %>% str_split(pattern = ',',simplify = T) %>% 
      as.numeric 
    a2=deter[i,4] %>% str_split(pattern=',',simplify = T) %>% 
      as.numeric
    d=c(d,dist2Line(p = point %>% as.numeric,line = rbind(a1,a2))[,1])
  }
  p=which.min(d) %>% as.numeric
  return(deter[p,1:2])
}
Search_fun=function(dat){
  b=read.csv(paste0(save_keypath,'key.csv'),
             stringsAsFactors = F) %>% .[,1]
  p1=which(colnames(dat) == 'callsign')
  p2=which(colnames(dat) == 'longitude')
  p3=which(colnames(dat) == 'latitude')
  dat[,p1]=gsub(pattern = '\\s+',replacement = '',x = dat[,p1])
  #去除沒有callsign的dat
  p0=which(dat[,p1] == "")
  if(length(p0) == 0){
    print('good')
  }else{
    dat1=dat
    dat=dat[-p0,]
  }
  #區分成兩部分，已經有的callsign與沒有的
  have=which(dat[,p1] %in% b) %>% dat[.,p1]
  have_not=setdiff(dat[,p1],have)
  #現有
  if(length(have) >0){
    D_have=data.frame(callsign=have,
                      from=NA,
                      to=NA,stringsAsFactors = F)
    for(i in 1:length(have)){
      table_path=paste0(save_path,have[i],'.csv')
      code='Big5'
      table_data=read.csv(file = table_path,
                          fileEncoding = code,header = T,
                          stringsAsFactors = F)
      #如果全部起降地都一樣，直接回傳第一筆資料
      if(((!(table_data[,2:3] %>% duplicated())) %>% 
          table_data[.,] %>%
          dim %>% .[1]) == 1){
        D_have[i,2:3]=table_data[1,2:3]
      }else{ #否則用經緯度到兩兩直線的距離，看哪條直線是最近的，
        # 就回傳此機場
        deter=(!(table_data[,2:3] %>% duplicated())) %>%
          table_data[.,2:3]
        setDF(deter)
        deter[,3]=Search_longlat(deter[[1]])
        deter[,4]=Search_longlat(deter[[2]])
        colnames(deter)[3:4]=c('depar_longlat','arrival_longlat')
        #找不到機場經緯度，刪除並警告
        p=which(is.na(deter[,3])|is.na(deter[,4]))
        if(length(p)>0){
          warning(paste0('Can not find ',deter[p,1],' or ',
                         deter[p,2],
                         ' long & lat,please renew airport_long_lat.'))
          deter=deter[-p,]
        }
        if(dim(deter)[1] ==1){
          D_have[i,2:3]=deter[,1:2]
        }else if(dim(deter)[1] == 0){
          D_have[i,2:3]=c(NA,NA)
        }else{
          D_have[i,2:3]=
            dist_self(point = c(dat[i,p2],dat[i,p3]),
                      deter = deter)
        }
      }
    }
    print('Done half')
  }else{
    print('Done half')
  }
  #需要新增
  if(length(have_not)>0){
    D_havenot=data.frame(callsign=have_not,
                         from=NA,
                         to=NA,stringsAsFactors = F)
    past_nonavaiable=
      read.csv(paste0(save_keypath,'key_nonavaiable.csv'),
               stringsAsFactors = F,h=T) %>% .[,1]
    p=which(!(have_not %in% past_nonavaiable))
    if(length(p)>0){
      can_not=Add_callsign(have_not[p],b) #回傳無法獲得的key
      if(length(can_not)>0){
        P=which(D_havenot[,1] %in% c(past_nonavaiable,can_not))
        D_havenot[P,2:3]=NA
        have_not=have_not[-P]
      }else print('No need to update nonavaiable key')
    }else{
      P=which(D_havenot[,1] %in% past_nonavaiable)
      D_havenot[P,2:3]=NA
      have_not=have_not[-P]
    }
    if(length(have_not) != 0){
      for(i in 1:length(have_not)){
        table_path=paste0(save_path,have_not[i],'.csv')
        code='Big5'
        table_data=read.csv(file = table_path,
                            fileEncoding = code,h=T,
                            stringsAsFactors = F)
        #如果全部起降地都一樣，直接回傳第一筆資料
        if(((!(table_data[,2:3] %>% duplicated())) %>% 
            table_data[.,] %>%
            dim %>% .[1]) == 1){
          p=which(D_havenot[,1] == have_not[i])
          D_havenot[p,2:3]=table_data[1,2:3]
        }else{ #否則用經緯度到兩兩直線的距離，看哪條直線是最近的，
          # 就回傳此機場
          deter=(!(table_data[,2:3] %>% duplicated())) %>%
            table_data[.,2:3]
          setDF(deter)
          deter[,3]=Search_longlat(deter[[1]])
          deter[,4]=Search_longlat(deter[[2]])
          colnames(deter)[3:4]=c('depar_longlat','arrival_longlat')
          #找不到機場經緯度，刪除並警告
          p=which(is.na(deter[,3])|is.na(deter[,4]))
          if(length(p)>0){
            warning(paste0('Can not find ',deter[p,1],' or ',deter[p,2],
                           ' long & lat,please renew airport_long_lat.'))
            deter=deter[-p,]
          }
          if(dim(deter)[1] ==1){
            p=which(D_havenot[,1] == have_not[i])
            D_havenot[p,2:3]=deter[,1:2]
          }else if(dim(deter)[1] == 0){
            p=which(D_havenot[,1] == have_not[i])
            D_havenot[p,2:3]=c(NA,NA)
          }else{
            p=which(D_havenot[,1] == have_not[i])
            D_havenot[p,2:3]=
              dist_self(point = c(dat[i,p2],dat[i,p3]),
                        deter = deter)
          }
        }
      }
    }else{
      print('Done')
    }
  }
  if(length(p0) == 0){
    if(length(have)>0 & length(have_not)>0){
      D_total=rbind(D_have,D_havenot)
      left_join(dat,D_total,by='callsign') %>% .[,18:19] %>% 
        toJSON() %>%
        return
    }else if(length(have)>0 & length(have_not) ==0){
      left_join(dat,D_have,by='callsign') %>% .[,18:19] %>% toJSON() %>%
        return
    }else if(length(have) ==0 & length(have_not)>0){
      left_join(dat,D_havenot,by='callsign') %>% .[,18:19] %>% toJSON() %>%
        return 
    }else{
      stop('error for data')
    } 
  }else{
    if(length(have)>0 & length(have_not)>0){
      D_total=rbind(D_have,D_havenot)
      left_join(dat1,D_total,by='callsign') %>% .[,18:19] %>% 
        toJSON() %>%
        return
    }else if(length(have)>0 & length(have_not) ==0){
      left_join(dat1,D_have,by='callsign') %>% .[,18:19] %>% 
        toJSON() %>%
        return
    }else if(length(have) ==0 & length(have_not)>0){
      left_join(dat1,D_havenot,by='callsign') %>% .[,18:19] %>% 
        toJSON() %>%
        return 
    }else{
      stop('error for data')
    } 
  }
}

