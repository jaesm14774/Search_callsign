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
save_path='C:/data/test/'
save_keypath='C:/data/test_key/'
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

