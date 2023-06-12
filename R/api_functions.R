#This script contains API functions for downloading and processing data from dewey API

get_token <- function(){
  
  require(RCurl)
  require(httr)
  require(jsonlite)
  #Prompt user for username and password
  {
    uname=readline(prompt = "email:")
    pwd=readline(prompt = "password:")
  }
  
  #Calling the API to request a token
  # check <- POST(url="https://marketplace.deweydata.io/api/auth/tks/get_token",
  #               add_headers(accept= 'application/json',
  #                           Authorization= str_c('Basic ',jsonlite::base64_enc("jbayham@colostate.edu:Imkaahpi2006!"))))
  check <- POST(url="https://marketplace.deweydata.io/api/auth/tks/get_token",
                add_headers(accept= 'application/json',
                            Authorization= paste0('Basic ',jsonlite::base64_enc(paste0(uname,":",pwd)))))
  
  if(check$status_code==401) stop("Unauthorized: check credentials")
  
  #Extract token from reply
  json_token = fromJSON(rawToChar(check$content)) 
  token=json_token$access_token
  
  return(token)
}


api_list_files <- function(access_token=NULL,read_cache=TRUE){
  
  require(magrittr)
  require(dplyr)
  require(purrr)
  require(lubridate)
  
  #Check if there is a cached file list
  fn="R/cache/file_list.csv"
  if(read_cache & file.exists(fn)){
    cdate=as_date(as_datetime(file.info(fn)$mtime))
    if(cdate==today()){
      file_list <- read.csv(fn)
    } else {
      stop(paste0("file_list was last cached on ",cdate,". Set read_cache=FALSE to rebuild file_list."))
    }
    
  } else {
  
    message("Rebuilding file_list. Be patient.")
    #Need to recursively walk through directory structure 
    send_headers = c('accept' = 'application/json','Authorization' = paste0('Bearer ',access_token))
    base_url = "https://marketplace.deweydata.io"
    level=0
    
    #GET initial request to my dewey root directory
    request <- GET(url=paste0(base_url,"/api/data/v2/list/"),headers = send_headers)
    
    #parse request to get initial df
    df_temp <- rawToChar(request$content) %>% fromJSON()
    
    temp_dirs <- df_temp %>%
      filter(directory) %>%
      mutate(level=level)
    
    all_files =vector("list")
    
    while (request$status_code==200 & nrow(temp_dirs)>0) {
      #Set directory level
      level=level + 1
      
      message(paste0("Crawling directory level ",level))
      
      #Crawling through directories
      request_df <- temp_dirs$url %>%
        map_dfr(function(x){
          #GET initial request to my dewey root directory
          request <- GET(url=paste0(base_url,x),headers = send_headers)
          #parse request to get initial df
          df_temp <- rawToChar(request$content) %>% fromJSON()
          return(df_temp)
        }) %>%
        mutate(level=level)
      
      temp_dirs <- request_df %>%
        filter(directory)
      
      temp_files <- request_df %>%
        filter(!directory)
      
      if(nrow(temp_files)>0){
        all_files[[level]] <- temp_files
      }
      
    }
    
    #Remove empty entries and convert to dataframe
    file_list <- compact(all_files) %>%
      bind_rows()
    
    #Check whether cache directory exists; if not, create
    if(!dir.exists("R/cache")) dir.create("R/cache",recursive = T)
    
    #cache file list in csv
    write.csv(file_list,fn,row.names = F)
  
  }
  
  return(file_list)
}

#function to download files using curl
bdown=function(url, file){
  library(RCurl)
  f = CFILE(file, mode="wb")
  a = curlPerform(url = url, writedata = f@ref, noprogress=FALSE,httpheader=send_headers)
  close(f)
  return(a)
}
