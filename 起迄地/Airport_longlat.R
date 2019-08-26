library(rvest)
library(dplyr)
library(jsonlite)
library(stringr)
library(lubridate)
library(readr)
library(gtools)
#讀取已儲存機場
Data=read.csv('C:/Users/lenovon/Desktop/飛機相關/airport_long_lat1.csv',
              stringsAsFactors = F,h=T)
#parameter callsign
#爬outairport機場資訊
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
    p=grepl(pattern = '([A-Z0-9]{3} )*([A-Z0-9]{4})+',x = temp) 
    temp=temp[p]
    code=str_split(string = temp[1],pattern = ' ',simplify = T)
    if(identical(nchar(code) %>% as.numeric(),c(3,4))){
      code=c(code[1],a[i])
    }else if(identical(nchar(code) %>% as.numeric(),c(4))){
      code=c(NA,a[i])
    }else if(identical(nchar(code) %>% as.numeric(),c(3,4,3))){
      code=c(code[1,3],a[i])
    }else if(identical(nchar(code) %>% as.numeric(),c(3))){
      code=c(NA,a[i])
    }else{
      next()
    }
    long=temp[3] %>% str_split(pattern = '\\|',simplify = T) %>% 
      .[1] %>%
      as.numeric
    lat=temp[2] %>% str_split(pattern = '\\|',simplify = T) %>% 
      .[1] %>%
      as.numeric
    D=rbind(D,data.frame(ICAO=code[2],IATA=code[1],long=long,lat=lat))
  }
  return(D)
}
#a is airport callsign you want to find
D=Airport_longlat(a)
Data=rbind(Data,D)
Data=distinct(Data)
Data=Data %>% arrange(ICAO)
rownames(Data)=1:dim(Data)[1]

write_csv(x = Data,
          path = 'C:/Users/lenovon/Desktop/飛機相關/airport_long_lat1.csv')
