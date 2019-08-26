library(rvest)
library(dplyr)
library(jsonlite)
library(stringr)
library(lubridate)
library(data.table)
library(readr)
library(gtools)
G=read_html('https://opensky-network.org/apidoc/rest.html') %>%
  html_nodes('table') %>% html_table() 
col_data=G[[4]]
col_data=col_data[,-1] #remove index

#parameter
restricted_url='https://opensky-network.org/api/states/all?lamin=22&lomin=120&lamax=25&lomax=122'
record_path='C:/Users/jaesm14774/Desktop/instant_flight/'
threshold=1.5 # unit is hours
#find flight from & to
track_fun=function(callsign){
  url=paste0('https://zh-tw.flightaware.com/live/flight/',callsign %>% toupper())
  to_from=
    read_html(url) %>%
    html_nodes('.flightPageAdBottom.flightPageNewAdUnit') %>% html_nodes('.flightPageAdUnit') %>%
    html_text(trim=T) %>% as.character() %>% str_extract_all(pattern = 'IATA.{8}',simplify = T) %>%
    str_extract_all("\\'[A-Za-z]{2,3}\\'",simplify = T) %>% gsub(pattern="\\'",replacement = "",x = .)
  if(dim(to_from)[1] == 0 | dim(to_from)[2] == 0){
    return(c(NA,NA))
  }
  from=to_from[2,1]
  to=to_from[1,1]
  return(c(from,to))
}
#create temp.csv, this file is main file
i=list.files(record_path,'csv',recursive = F,full.names = T)
if(length(i) == 0){
  write_csv(x =data.frame(icao24=character(),callsign=character(),
                          dt_in=character(),dt_out=character(),
                          from=character(),to=character()),
            path=paste0(record_path,'temp.csv'))
}else if(!(i %in% paste0(record_path,'temp.csv'))){
  write_csv(x =data.frame(icao24=character(),callsign=character(),
                          dt_in=character(),dt_out=character(),
                          from=character(),to=character()),
            path=paste0(record_path,'temp.csv'))
}else{
  print('do nothing')
}
#split temp into date for finish data
split_date=function(dir_path,finish_data){
  finish_data=finish_data %>% mutate(dt_in=dt_in %>% as.POSIXct()) %>%
    arrange(dt_in)
  finish_data[,3]=finish_data[,3] %>% as.character()
  date=as_date(finish_data[,3]) %>% unique %>% as.character()
  deter=list.files(dir_path,pattern = '.csv',full.names = T,recursive = F)
  for(name in date){
    p=which(as_date(finish_data[,3]) == name)
    if(!(paste0(dir_path,name,'.csv') %in% deter)){
      write_csv(x = finish_data[p,],path = paste0(dir_path,name,'.csv'))
    }else{
      write_csv(x = finish_data[p,],path = paste0(dir_path,name,'.csv'),append=T)
    }
  }
}
Crawler_func=function(restricted_url,record_path,threshold){
  data_url=restricted_url
  data=fromJSON(data_url)$states %>% data.frame(stringsAsFactors=F)
  colnames(data)=col_data[,1]
  P=which(colnames(data) == 'last_contact')
  data[,P]=data[,P] %>% 
    as.numeric %>%
    as.POSIXct(origin='1970-01-01') %>% as.character()
  #write data to temp.csv
  dir_path=record_path
  record_path=paste0(record_path,'temp.csv')
  #record format : 1.icao24 2.callsign 3.dt_in 4.dt_out 5.from 6.to
  record=read.csv(record_path,h=T,stringsAsFactors = F)
  p1=which(colnames(data) == 'icao24')
  p2=which(colnames(data) == 'callsign')
  if(dim(record)[1] == 0){
    G=sapply(data[,p2],track_fun) %>% t
    record=data.frame(icao24=data[,p1],
                      callsign=data[,p2],
                      dt_in=data[,P],
                      dt_out=NA,
                      from=G[,1],
                      to=G[,2],
                      stringsAsFactors = F)
    write_csv(path = record_path,x = record)
    return('Done')
  }else{
    temp_d=data[,p1] 
    temp_r=record[,1] %>% unique
    d_in_r=which(temp_d %in% temp_r)
    d_notin_r=which(!(temp_d %in% temp_r))
    r_in_d=which(temp_r %in% temp_d)
    r_notin_d=which(!(temp_r %in% temp_d))
    #condition I (check if it is new record,and insert it)
    if(length(d_in_r) != 0){
      change=NULL
      for(i in 1:length(d_in_r)){
        p=which(record[,1] == data[d_in_r[i],p1]) %>% max #prevent duplicated id
        change=c(change,interval(record[p,3],data[d_in_r[i],P]) %>%
                   time_length('hours'))
      }
      change=which(change >threshold)#Enter Taiwan in the past,
      #and it is new record.
      if(length(change) >0){
        G=sapply(data[change,p2],track_fun) %>% t
        write_csv(path=record_path,x=data.frame(icao24=data[d_in_r[change],p1],
                                                callsign=data[d_in_r[change],p2],
                                                dt_in=data[d_in_r[change],P],
                                                dt_out=NA,
                                                from=G[,1],
                                                to=G[,2]),
                  append = T)
      }
    }
    #condition II (insert flight out time)
    if(length(r_notin_d) !=0 ){
      if(sum(is.na(record[r_notin_d,4])) >0){#ensure some data need to insert out_time
        #record is not in data and satisfied out_time=NA
        temp_r=record[,1]
        p=which((!(temp_r %in% temp_d)) & (is.na(record[,4]))) 
        record[p,4]=lubridate::now() %>% as.character()
        write_csv(x = record[p,],path = record_path,append = T)
        #clean duplicated record 
        temp=read.csv(record_path,header = T,stringsAsFactors = F)
        #candidate
        temp=temp[!duplicated(temp[,1:3],fromLast = T),]
        write_csv(x=temp,path=record_path)
      } 
    }
    #condition III (insert in record)
    if(length(d_notin_r) !=0){
      G=sapply(data[d_notin_r,p2],track_fun) %>% t
      write_csv(path= record_path,x = data.frame(icao24=data[d_notin_r,p1],
                                                 callsign=data[d_notin_r,p2],
                                                 dt_in=data[d_notin_r,P],
                                                 dt_out=NA,
                                                 from=G[,1],
                                                 to=G[,2]),
                append = T)
    }
    #condition IV (nothing)
    print('Step1 done')
  }
  #Split Ok data into others file and remove it in temp to save space
  record=read.csv(record_path,header = T,stringsAsFactors = F)
  p=which(!is.na(record[,4]))
  if(length(p) ==0){
    rownames(record)=1:dim(record)[1]
    write_csv(path= record_path,x=record)
  }else{
    split_date(dir_path,record[p,,drop=F])
    record=record[-p,]
    rownames(record)=1:dim(record)[1]
    write_csv(path= record_path,x=record) 
  }
  print('Step2 done')
  return('All Done')
}

n=1
while(n>0){
  Crawler_func(restricted_url,record_path,threshold)
  print(n)
  n=n+1
  Sys.sleep(runif(1,5,20))
}
