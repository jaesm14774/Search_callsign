library(data.table)
library(rvest)
library(dplyr)
library(stringr)
library(tidytext)
library(jiebaR)
library(tibble)
library(jsonlite)
library(lubridate)
library(readr)
library(gtools)
options(scipen = 999)
DATA=read.csv(file = 'C:/Users/jaesm14774/Desktop/airport_long_lat1.csv',
              stringsAsFactors = F,h=T)
all_file=list.files(path = 'C:/data/test',pattern = '.csv',full.names = T,recursive = F)
data=NULL
for(i in 1:length(all_file)){
  a=read.csv(all_file[i],stringsAsFactors = F)
  a=tryCatch(distinct(a,出發地,目的地),warning=function(w) stop('w'),
             error=function(e) stop('error'))
  a=c(a[,1],a[,2])
  b=str_split(string = a,pattern = ' \\(')
  b=sapply(X = b,FUN = function(x) return(x[length(x)]))
  a=str_extract(string = b,pattern = '[0-9A-Z]{4}')
  data=c(data,unique(a))
}
data=unique(data)
data=data[!is.na(data)]
data=data[which(!(data %in% DATA[,1]))]
Airport_longlat=function(callsign){
  a=callsign
  D=NULL
  for(i in 1:length(a)){
    temp=
      tryCatch(
        read_html(paste0('http://ourairports.com/airports/',a[i],'/pilot-info.html#general')) %>%
          html_nodes('dd') %>% html_nodes('p') %>% html_text(trim=T),
        error=function(e){
          return(0)
        }
      )
    if(class(temp) == 'numeric'){
      next()
    }
    p=grepl(pattern = '([A-Z0-9]{3} )*([A-Z0-9]{4})+',x = temp)  %>% which
    if(length(p) < 3){
      p=c(1,p)
    }
    temp=temp[p]
    code=str_split(string = temp[1],pattern = ' ',simplify = T)
    if(identical(nchar(code) %>% as.numeric(),c(3,4))){
      code=code
    }else if(identical(nchar(code) %>% as.numeric(),c(4))){
      code=c(NA,a[i])
    }else if(identical(nchar(code) %>% as.numeric(),c(3,4,3))){
      code=c(code[1,3],a[i])
    }else if(identical(nchar(code) %>% as.numeric(),3)){
      code=c(NA,a[i])
    }else{
      next()
    }
    long=temp[3] %>% str_split(pattern = '\\|',simplify = T) %>% .[1] %>%
      as.numeric
    lat=temp[2] %>% str_split(pattern = '\\|',simplify = T) %>% .[1] %>%
      as.numeric
    D=rbind(D,data.frame(ICAO=code[2],IATA=code[1],long=long,lat=lat,stringsAsFactors = F))
  }
  return(D)
}
if(length(data) !=0){
 d=Airport_longlat(data) 
}else d=NULL
#double check because redirect for website probably
p=which(!(d[,1] %in% DATA[,1]))
if(length(p)>0){
  d=d[p,]
}else d=NULL
#append new data to airport data
DATA=rbind(DATA,d)
write.csv(x = DATA,file = 'C:/Users/jaesm14774/Desktop/airport_long_lat1.csv',
          row.names = F)
