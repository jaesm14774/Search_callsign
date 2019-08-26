library(data.table)
library(rvest)
library(dplyr)
library(stringr)
library(tidytext)
library(jiebaR)
library(tibble)
library(RSQLite)
library(jsonlite)
library(lubridate)
library(readr)
library(RMySQL)
library(gtools)
db=dbConnect(drv =MySQL(),user='mariadb',password='mariadb@NCHC',
             port=3306,host='127.0.0.1',dbname='test')
past_record_path='C:/Users/jaesm14774/Desktop/military_aircraft/'
#crawl data function
Crawl_airline=function(past_record_path){
  temp=read_html('https://www.adsbexchange.com/airborne-military-aircraft/')
  d1=
    temp %>% html_nodes('table') %>% .[1] %>% html_table()
  d1=d1[[1]]
  d1=d1[,-7]
  d1[,8]=NA
  d1[,9]=NA
  colnames(d1)[8:9]=c('Lat','Lon')
  d1=d1[,c(1:6,8,9,7)]
  txt=
    temp %>%
    html_nodes('#MilitaryAircraft > tbody') %>%
    html_nodes('tr') %>% html_nodes('td') %>%
    html_nodes('a') 
  txt=as.character(txt)
  txt=str_detect(txt,pattern = 'tr|br') %>% txt[.]
  lat_lon=str_match_all(txt,'>-*\\d+\\.\\d+') 
  for(i in 1:length(lat_lon)){
    if(length(lat_lon[[i]]) ==2){
      d1[i,7]=lat_lon[[i]][1] %>% gsub(pattern ='>',replacement = "",x=.)
      d1[i,8]=lat_lon[[i]][2] %>% gsub(pattern = '>',replacement = "",x=.)
    }else d1=d1
  }
  d1=d1[which(d1[,1] != ""),]
  d1=d1[!is.na(d1[,7]),]
  d1=d1[!is.na(d1[,8]),]
  rownames(d1)=1:(dim(d1)[1])
  colnames(d1)=c("Operator","Model","Reg","Callsign","Speed","Alt","Lat","Lon","ICAO-hex")
  dbWriteTable(db,'phone',d1,overwrite=T)
  DET=now() %>% as.Date %>% as.character()
  deter=list.files(past_record_path,'csv',recursive = F,full.names = T)
  if(length(deter) == 0){
    write_csv(x =d1,path = paste0(past_record_path,DET,'.csv'))
  }else{
    deter=deter[length(deter)] %>% 
      str_match(string = .,pattern = '/\\d+-\\d+-\\d+') %>% gsub(pattern = '/',replacement = '',x = .)%>%
      as.character()
    if(deter == DET){
      write_csv(x = d1,path = paste0(past_record_path,deter,'.csv'),append=T)
    }else{
      write_csv(x = d1,path = paste0(past_record_path,DET,'.csv'))
    }
  }
  print('Done')
}
n=1
while(n>0){
  temp=read_html('https://www.adsbexchange.com/airborne-military-aircraft/')
  d1=
    temp %>% html_nodes('table') %>% .[1] %>% html_table()
  d1=d1[[1]]
  d1=d1[,-7]
  d1[,8]=NA
  d1[,9]=NA
  colnames(d1)[8:9]=c('Lat','Lon')
  d1=d1[,c(1:6,8,9,7)]
  txt=
    temp %>%
    html_nodes('#MilitaryAircraft > tbody') %>%
    html_nodes('tr') %>% html_nodes('td') %>%
    html_nodes('a') 
  txt=as.character(txt)
  txt=str_detect(txt,pattern = 'tr|br') %>% txt[.]
  lat_lon=str_match_all(txt,'>-*\\d+\\.\\d+') 
  for(i in 1:length(lat_lon)){
    if(length(lat_lon[[i]]) ==2){
      d1[i,7]=lat_lon[[i]][1] %>% gsub(pattern ='>',replacement = "",x=.)
      d1[i,8]=lat_lon[[i]][2] %>% gsub(pattern = '>',replacement = "",x=.)
    }else d1=d1
  }
  d1=d1[which(d1[,1] != ""),]
  d1=d1[!is.na(d1[,7]),]
  d1=d1[!is.na(d1[,8]),]
  rownames(d1)=1:(dim(d1)[1])
  colnames(d1)=c("Operator","Model","Reg","Callsign","Speed","Alt","Lat","Lon","ICAO-hex")
  dbWriteTable(db,'phone',d1,overwrite=T)
  DET=now() %>% as.Date %>% as.character()
  deter=list.files(past_record_path,'csv',recursive = F,full.names = T)
  if(length(deter) == 0){
    write_csv(x =d1,path = paste0(past_record_path,DET,'.csv'))
  }else{
    deter=deter[length(deter)] %>% 
      str_match(string = .,pattern = '/\\d+-\\d+-\\d+') %>% gsub(pattern = '/',replacement = '',x = .)%>%
      as.character()
    if(deter == DET){
      write_csv(x = d1,path = paste0(past_record_path,deter,'.csv'),append=T)
    }else{
      write_csv(x = d1,path = paste0(past_record_path,DET,'.csv'))
    }
  }
  print('Done')
  n=n+1
  Sys.sleep(runif(1,10,40))
  print(Sys.time())
  print(n)
}
dbDisconnect(db)
