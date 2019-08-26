library(rvest)
library(dplyr)
library(jsonlite)
library(stringr)
library(lubridate)
library(readr)
library(data.table)
library(gtools)
G=read_html('https://opensky-network.org/apidoc/rest.html') %>%
  html_nodes('table') %>% html_table() 
col_data=G[[4]]
col_data=col_data[,-1] #remove index

#parameter
url='https://jaesm14774sky:zxcasdqwe@opensky-network.org/api/states/all'
record_path='C:/Users/jaesm14774/Desktop/long_lat/'
threshold=0.5 # unit is hours

#revise the right json form for data
FINAL=function(dat){
  #Json cannot recognize NA
  dat[,5]=gsub(pattern = 'NA',replacement = 'null',x = dat[,5])
  #determine it is new record or not 
  deter=
    ((interval(dat[,3] %>% as.POSIXct(),now()) %>% time_length('hours')) > threshold) &
    !(is.na(dat[,4]))
  deter=grepl(',$',x = dat[,5]) & deter
  if(length(which(deter)) >0){
    dat[deter,5]=gsub(pattern = ',$',replacement = ']}',x = dat[deter,5])
    return(dat)
  }else return(dat)
}
#create temp.csv, this file is main file
i=list.files(record_path,'csv',recursive = F,full.names = T)
if(length(i) == 0){
  write_csv(x =data.frame(icao24=character(),callsign=character(),
                          time_in=character(),time_out=character(),
                          long_lat=character()),
            path=paste0(record_path,'temp.csv'))
}else if(!(i %in% paste0(record_path,'temp.csv'))){
  write_csv(x =data.frame(icao24=character(),callsign=character(),
                          time_in=character(),time_out=character(),
                          long_lat=character()),
            path=paste0(record_path,'temp.csv'))
}else{
  print('do nothing')
}
#split temp into date for finish data
split_date=function(dir_path,finish_data){
  finish_data=finish_data %>% mutate(time_in=time_in %>% as.POSIXct()) %>%
    arrange(time_in)
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
#main function
Crawler_func=function(url,record_path,threshold){
  data_url=url
  data=fromJSON(data_url)$states %>% data.frame(stringsAsFactors=F)
  colnames(data)=col_data[,1]
  P=c(1,2,5,6,7)
  data=data[,P]
  data[,3]=data[,3] %>% 
    as.numeric %>%
    as.POSIXct(origin='1970-01-01') %>% as.character()
  #write data to temp.csv
  dir_path=record_path
  record_path=paste0(record_path,'temp.csv')
  #record format : 1.icao24 2.callsign 3.time_in 4.time_out 5.long_lat
  record=read.csv(record_path,h=T,stringsAsFactors = F)
  p1=which(colnames(data) == 'icao24')
  p2=which(colnames(data) == 'callsign')
  if(dim(record)[1] == 0){
    record=data.frame(icao24=data[,p1],
                      callsign=data[,p2],
                      time_in=data[,3],
                      time_out=NA,
                      long_lat=paste0('{"long_lat":[[',data[,4],",",data[,5],'],'),
                      stringsAsFactors = F)
    write_csv(path = record_path,x = record)
    return('Done')
  }else{
    setDT(record)
    setkey(record,icao24)
    setDF(record)
    temp_d=data[,p1] 
    temp_r=record[,1] %>% unique
    d_in_r=which(temp_d %in% temp_r)
    d_notin_r=which(!(temp_d %in% temp_r))
    r_in_d=which(temp_r %in% temp_d)
    r_notin_d=which(!(temp_r %in% temp_d))
    CHANGE=NULL #重要!記錄所有record修改的位置
    #condition I (check if it is new record,and insert it)
    if(length(d_in_r) != 0){
      change=NULL
      setDT(record)
      setkey(record,icao24)
      temp_1=record[data[d_in_r,1],]
      temp_1=temp_1[!duplicated(temp_1[,1],fromLast = T),] %>% 
        data.frame(stringsAsFactors = F)
      change=interval(temp_1[,4],data[d_in_r,3]) %>%
        time_length('hours')
      # for(i in 1:length(d_in_r)){
      #   p=which(record[,1] == data[d_in_r[i],p1]) %>% max #prevent duplicated id
      #   change=c(change,interval(record[p,4],data[d_in_r[i],3]) %>%
      #              time_length('hours'))
      # }
      ##Appear in the past,but it is new record due to time length
      change=which((!is.na(change)) & (change > threshold))
      #增加新的經緯度座標到record
      not_change=setdiff(d_in_r,change)
      temp_2=record[data[not_change,1],]
      temp_2=temp_2[!(duplicated(temp_2[,1],fromLast = T)),] %>%
        data.frame(stringsAsFactors = F)
      temp_2[,5]=paste0(temp_2[,5],'[',data[not_change,4],",",
                        data[not_change[5],5],'],')
      CHANGE=c(CHANGE,temp_2[,1])
      # for(i in 1:length(not_change)){
      #   p=which(record[,1] == data[not_change[i],p1]) %>% max
      #   record[p,5]=paste0(record[p,5],'[',data[not_change[i],4],",",
      #                      data[not_change[i],5],'],')
      #   CHANGE=c(CHANGE,p) #紀錄修改第一次
      # }
      #相差超過threshold小時，代表這些舊icao24是新紀錄
      if(length(change) >0){
        #不用紀錄修改，因為寫進去檔案了
        write_csv(path=record_path,x=data.frame(icao24=data[d_in_r[change],p1],
                                                callsign=data[d_in_r[change],p2],
                                                time_in=data[d_in_r[change],3],
                                                time_out=NA,
                                                long_lat=paste0('{"long_lat":[[',data[d_in_r[change],4],",",data[d_in_r[change],5],'],'),
                                                stringsAsFactors = F),
                  append = T)
      }
    }
  
    #預防有飛機在雷達消失後，又出現
    DETER=interval(temp_2[,4],data[not_change,3]) %>%
      time_length('hours')
    p=which((!is.na(DETER)) & DETER>0)
    if(length(p) !=0){
      CHANGE=c(CHANGE,temp_2[p,1])
      temp_2[p,4]=NA
    }
    #用temp更新record資料(不會QQ)
    p=which(!(record[temp_2[,1],][[1]] %>% duplicated(fromLast=T)))
    record[temp_2[,1],][p,]=temp_2
    setDF(record)
    # for(i in 1:length(not_change)){
    #   p=which(record[,1] == data[not_change[i],p1]) %>% max
    #   DETER=interval(record[p,4],data[not_change[i],3]) %>% 
    #     time_length('hours')
    #   if((!is.na(DETER)) & DETER>0){
    #     CHANGE=c(CHANGE,p) #紀錄修改第二次
    #     record[p,4]=NA
    #   }  
    # }
    
    #condition II (insert flight out time)
    if(length(r_notin_d) !=0 ){
      if(sum(is.na(record[r_notin_d,4])) >0){#ensure some data need to insert time_out
        #record is not in data and satisfied time_out=NA
        temp_r=record[,1]
        p=which((!(temp_r %in% temp_d)) & (is.na(record[,4]))) %>% record[.,1]
        CHANGE=c(CHANGE,p) #紀錄修改第三次
        setDT(record)
        setkey(record,icao24)
        record[,time_out:=as.character(time_out)]
        record[p,][[4]]=lubridate::now() %>% as.character()
        write_csv(x = record[CHANGE,],path = record_path,append = T)
        #clean duplicated record 
        temp=read.csv(record_path,header = T,stringsAsFactors = F)
        #candidate
        temp=temp[!duplicated(temp[,1:3],fromLast = T),]
        write_csv(x=temp,path=record_path)
      } 
    }
    #condition III (insert surely new record)
    if(length(d_notin_r) !=0){
      write_csv(path= record_path,x = data.frame(icao24=data[d_notin_r,p1],
                                                 callsign=data[d_notin_r,p2],
                                                 time_in=data[d_notin_r,3],
                                                 time_out=NA,
                                                 long_lat=paste0('{"long_lat":[[',data[d_notin_r,4],",",data[d_notin_r,5],'],'),
                                                 stringsAsFactors = F),
                append = T)
    }
    
    #condition IV (nothing)
    print('Step1 Done')
  }
  record=read.csv(record_path,header = T,stringsAsFactors = F)
  record=FINAL(record)
  p=which(grepl(pattern = ']}',x = record[,5]))
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
while(n<10){
  Crawler_func(url,record_path,threshold)
  Sys.sleep(runif(1,10,25))
  n=n+1
  if(n %% 25) print(n)
}


