library(RSQLite)
library(rvest)
library(dplyr)
library(jsonlite)
library(stringr)
library(lubridate)
library(readr)
library(RMySQL)
library(gtools)
library(mgsub)
library(data.table)
#parameters
#file of path where we want to save aircraft_information 
path_save='C:/Users/jaesm14774/Desktop/private_aircraft/'
#file of path where we save airport_code_transform
path_airport_code='C:/Users/jaesm14774/Desktop/飛機相關/icao1.csv'
#file of path where we save aircraft database
path_aircraft='C:/Users/jaesm14774/Desktop/飛機相關/aircraftDatabase.csv'
#read airport code transform data
Database_Iata_to_Icao=read.csv(path_airport_code,h=T,
                               stringsAsFactors = F,
                               fileEncoding = 'Big5')
#read aircraft database from opensky network website (download)
Database_aircraft=fread(path_aircraft)
Database_aircraft=Database_aircraft[,c(1,5,6)]
#time for now and substract two day.Then transform to unix epoch for search
Time_end=(lubridate::now() %>% as.numeric %>% round)-86400*2
Time_start=Time_end-7200 #max range limit is two hours

options(scipen=999)
Crawler_aircraft_fun=function(path_save,Database_Iata_to_Icao,Database_aircraft,
                              Time_start,Time_end){
  data=fromJSON(paste0("https://USERNAME:PASSWORD@opensky-network.org/api/flights/all?begin=",Time_start,"&end=",Time_end))
  #icao24,callsign,departure,arrival,
  #departure_time,arrival_time,model,typecode
  data=data[,c(1,6,3,5,2,4)]
  #left join table to add model & typecode
  data=left_join(x=data,y=Database_aircraft,by='icao24')
  #change unix epoch to date
  data[,5]=data[,5] %>% as.numeric %>% 
    as.POSIXct(origin='1970-01-01') %>% as.character()
  
  data[,6]=data[,6] %>% as.numeric %>%
    as.POSIXct(origin='1970-01-01') %>% as.character()
  #create new csv or not
  DET=Time_start %>% as.POSIXct(origin='1970-01-01') %>%  lubridate::as_date()
  deter=list.files(path_save,'csv',recursive = F,full.names = T)
  if(length(deter) == 0){
    write_csv(x =data.frame(icao24=character(),callsign=character(),
                            ICAO_departure=character(),ICAO_arrival=character(),
                            departure_time=character(),arrival_time=character(),
                            model=character(),typecode=character()),
              path = paste0(path_save,DET,'.csv'))
    path_save=paste0(path_save,DET,'.csv')
  }else{
    deter=deter %>% .[length(deter)] %>% 
      str_match(string = .,pattern = '/\\d+-\\d+-\\d+') %>% gsub(pattern = '/',replacement = '',x = .)%>%
      as.character()
    if(deter == DET){
      path_save=paste0(path_save,deter,'.csv')
    }else{
      write_csv(x =data.frame(icao24=character(),callsign=character(),
                              ICAO_departure=character(),ICAO_arrival=character(),
                              departure_time=character(),arrival_time=character(),
                              model=character(),typecode=character()),
                path = paste0(path_save,DET,'.csv'))
      path_save=paste0(path_save,DET,'.csv')
    }
  }
  #Save record if it is not in the database of aircraft record
  record=read.csv(path_save,h=T,stringsAsFactors = F)
  colnames(data)=c('icao24','callsign','ICAO_departure','ICAO_arrival',
                   'departure_time','arrival_time',
                   'model','typecode')
  if(dim(record)[1] == 0){
    #dealt with transform airport code
    data[,9]=data[,3]
    colnames(data)[9]='ICAO'
    data=left_join(x=data,Database_Iata_to_Icao,by='ICAO')
    colnames(data)[10:13]=c('IATA_departure','airport_departure',
                            'city_departure','nation_departure')
    data[,9]=data[,4]
    data=left_join(x=data,y = Database_Iata_to_Icao,by='ICAO')
    colnames(data)[c(3,4,14:17)]=c('ICAO_departure','ICAO_arrival',
                                   'IATA_arrival','airport_arrival',
                                   'city_arrival','nation_arrival')
    data=data[,-9]
    #
    record=data
    write.csv(file = path_save,x = record,row.names = F)
  }else{
    #record new aircraft not in record or same aircraft but not same time
    temp=record[,1:8]
    p=which(duplicated(rbind(data,temp)[,c(1,2,5,6)],fromLast = T))
    if(length(p) != 0){
      new_record=data[-p,] 
    }else new_record=data
    if(dim(new_record)[1] ==0){
      return('No new data')
    }
    #dealt with transform airport code
    new_record[,9]=new_record[,3]
    colnames(new_record)[9]='ICAO'
    new_record=left_join(x=new_record,Database_Iata_to_Icao,by='ICAO')
    colnames(new_record)[10:13]=c('IATA_departure','airport_departure',
                            'city_departure','nation_departure')
    new_record[,9]=new_record[,4]
    new_record=left_join(x=new_record,y=Database_Iata_to_Icao,by='ICAO')
    colnames(new_record)[c(3,4,14:17)]=c('ICAO_departure','ICAO_arrival',
                                   'IATA_arrival','airport_arrival',
                                   'city_arrival','nation_arrival')
    new_record=new_record[,-9]
    fwrite(x = new_record,file = path_save,append = T,na = NA)
  }
  return('done')
}
n=1
#start=2019/6/26
start_date=as.POSIXct('2019-06-26 2:00:00 CST') %>% as.numeric
while(n<60){
  #time for now and substract two day.Then transform to unix epoch for search
  #Time_end=(lubridate::now() %>% as.numeric %>% round)-86400*2
  #Time_start=Time_end-7200 #max range limit is two hours
  Time_end=start_date
  Time_start=Time_end-7200 #max range limit is two hours
  tryCatch({Crawler_aircraft_fun(path_save=path_save,Time_start = Time_start,
                       Time_end=Time_end,
                       Database_Iata_to_Icao = Database_Iata_to_Icao,
                       Database_aircraft=Database_aircraft)},
           error=function(e) {
             Crawler_aircraft_fun(path_save=path_save,Time_start = Time_start,
                                  Time_end=Time_end,
                                  Database_Iata_to_Icao = Database_Iata_to_Icao,
                                  Database_aircraft=Database_aircraft)})
  n=n+1
  start_date=start_date+7200
  print(n)
  Sys.sleep(runif(1,2,6))
}

