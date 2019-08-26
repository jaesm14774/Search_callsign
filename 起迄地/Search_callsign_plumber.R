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
library(plumber)
source('C:/Users/jaesm14774/Desktop/Boss.R',encoding = 'UTF-8')


#* @apiTitle Boss 

#* @param callsign  Callsign you want to search
#* @param long Flight longitude now
#* @param lat  Flight latitude now
#* @get /Final
#* @post /Final
Search_fun=function(callsign,long,lat){
  if(length(callsign) != length(long) | 
     length(callsign) != length(lat)){
    stop('Error for not same length output')
  }else{
    dat=data.frame(callsign=callsign,
                   long=long %>% as.numeric,
                   lat=lat %>% as.numeric,stringsAsFactors = F)
  }
  b=read.csv(paste0(save_keypath,'key.csv'),
             stringsAsFactors = F) %>% .[,1]
  p1=1
  p2=2
  p3=3
  dat[,p1]=gsub(pattern = '\\s+',replacement = '',x = dat[,p1])
  #去除沒有callsign的dat
  p0=which(dat[,p1] == "" | is.na(dat[,p1]))
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
      code=SEARCH_ENCODE(table_path)
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
  #需要新增,但不新增
  if(length(have_not)>0){
    D_havenot=data.frame(callsign=have_not,
                         from=NA,
                         to=NA,stringsAsFactors = F)
  }else{
    print('Done')
  }
  if(length(p0) == 0){
    if(length(have)>0 & length(have_not)>0){
      D_total=rbind(D_have,D_havenot)
      rs_temp=left_join(dat,D_total,by='callsign') %>% .[,4:5]
      result=data.frame(from_name=rs_temp[,1],from_lon=NA,from_lat=NA,
                        to_name=rs_temp[,2],to_lon=NA,to_lat=NA,
                        stringsAsFactors = F)
      result[2:3]=Search_longlat(rs_temp[,1]) %>% str_split(pattern=',',simplify = T)
      result[5:6]=Search_longlat(rs_temp[,2]) %>% str_split(pattern=',',simplify = T)
      p=which(result[,3] == '')
      if(length(p)>0){
        result[p,3]=NA
      }else result=result
      p=which(result[,6] == '')
      if(length(p)>0){
        result[p,6]=NA
      }else result=result
      result %>% list %>% return
    }else if(length(have)>0 & length(have_not) ==0){
      rs_temp=left_join(dat,D_have,by='callsign') %>% .[,4:5]
      result=data.frame(from_name=rs_temp[,1],from_lon=NA,from_lat=NA,
                        to_name=rs_temp[,2],to_lon=NA,to_lat=NA,
                        stringsAsFactors = F)
      result[2:3]=Search_longlat(rs_temp[,1]) %>% str_split(pattern=',',simplify = T)
      result[5:6]=Search_longlat(rs_temp[,2]) %>% str_split(pattern=',',simplify = T)
      p=which(result[,3] == '')
      if(length(p)>0){
        result[p,3]=NA
      }else result=result
      p=which(result[,6] == '')
      if(length(p)>0){
        result[p,6]=NA
      }else result=result
      result %>% list %>% return
    }else if(length(have) ==0 & length(have_not)>0){
      rs_temp=left_join(dat,D_havenot,by='callsign') %>% .[,4:5]
      result=data.frame(from_name=rs_temp[,1],from_lon=NA,from_lat=NA,
                        to_name=rs_temp[,2],to_lon=NA,to_lat=NA,
                        stringsAsFactors = F)
      result[2:3]=Search_longlat(rs_temp[,1]) %>% str_split(pattern=',',simplify = T)
      result[5:6]=Search_longlat(rs_temp[,2]) %>% str_split(pattern=',',simplify = T)
      p=which(result[,3] == '')
      if(length(p)>0){
        result[p,3]=NA
      }else result=result
      p=which(result[,6] == '')
      if(length(p)>0){
        result[p,6]=NA
      }else result=result
      result %>% list %>% return
    }else{
      stop('error for data')
    } 
  }else{
    if(length(have)>0 & length(have_not)>0){
      D_total=rbind(D_have,D_havenot)
      rs_temp=left_join(dat1,D_total,by='callsign') %>% .[,4:5]
      result=data.frame(from_name=rs_temp[,1],from_lon=NA,from_lat=NA,
                        to_name=rs_temp[,2],to_lon=NA,to_lat=NA,
                        stringsAsFactors = F)
      result[2:3]=Search_longlat(rs_temp[,1]) %>% str_split(pattern=',',simplify = T)
      result[5:6]=Search_longlat(rs_temp[,2]) %>% str_split(pattern=',',simplify = T)
      p=which(result[,3] == '')
      if(length(p)>0){
        result[p,3]=NA
      }else result=result
      p=which(result[,6] == '')
      if(length(p)>0){
        result[p,6]=NA
      }else result=result
      result %>% list %>% return
    }else if(length(have)>0 & length(have_not) ==0){
      rs_temp=left_join(dat1,D_have,by='callsign') %>% .[,4:5]
      result=data.frame(from_name=rs_temp[,1],from_lon=NA,from_lat=NA,
                        to_name=rs_temp[,2],to_lon=NA,to_lat=NA,
                        stringsAsFactors = F)
      result[2:3]=Search_longlat(rs_temp[,1]) %>% str_split(pattern=',',simplify = T)
      result[5:6]=Search_longlat(rs_temp[,2]) %>% str_split(pattern=',',simplify = T)
      p=which(result[,3] == '')
      if(length(p)>0){
        result[p,3]=NA
      }else result=result
      p=which(result[,6] == '')
      if(length(p)>0){
        result[p,6]=NA
      }else result=result
      result %>% list %>% return
    }else if(length(have) ==0 & length(have_not)>0){
      rs_temp=left_join(dat1,D_havenot,by='callsign') %>% .[,4:5]
      result=data.frame(from_name=rs_temp[,1],from_lon=NA,from_lat=NA,
                        to_name=rs_temp[,2],to_lon=NA,to_lat=NA,
                        stringsAsFactors = F)
      result[2:3]=Search_longlat(rs_temp[,1]) %>% str_split(pattern=',',simplify = T)
      result[5:6]=Search_longlat(rs_temp[,2]) %>% str_split(pattern=',',simplify = T)
      p=which(result[,3] == '')
      if(length(p)>0){
        result[p,3]=NA
      }else result=result
      p=which(result[,6] == '')
      if(length(p)>0){
        result[p,6]=NA
      }else result=result
      result %>% list %>% return
    }else{
      stop('error for data')
    } 
  }
}

