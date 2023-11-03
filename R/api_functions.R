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

#access_token=jb_access_token
#endpoint=target_endpoint
api_list_files <- function(access_token=NULL,endpoint){
  
  require(httr)
  require(httr2)
  require(jsonlite)

  resp <- request(endpoint) %>%
    req_headers("Accept" = "application/json",
                "X-API-KEY"= access_token) %>% 
    req_perform()
  

  if(resp$status_code==200){
    #convert binary output to text
    out_list <- resp_body_json(resp)
    
    #grab the first 1000 rows of the links 
    out_df <- bind_rows(out_list$download_links)
    
    pg=out_list$page
    df_shell=vector("list")
    df_shell[[pg]]=out_df
    message(paste0("Looping through ",out_list$total_pages," pages"))
    while(pg<out_list$total_pages){ #if there are multiple pages, we need to grab the links from each page
      pg=pg+1
      resp <- request(endpoint) %>%
        req_headers("Accept" = "application/json",
                    "X-API-KEY"= access_token) %>% 
        req_url_query("page"=pg) %>%
        req_perform()
      
      out_list <- resp_body_json(resp)
      
      df_shell[[pg]] <- bind_rows(out_list$download_links)
      
      Sys.sleep(5) #5 second delay between calls
    }
  
  } else {
    stop(resp$status_code)
  }
  
  out <- bind_rows(df_shell)
  
  return(out)
}
  
  
#function to build out directory structure where data will be downloaded
build_dir <- function(target_dir){
  
  require(dplyr)
  require(stringr)
  require(purrr)
  
  #check whether parent target directory exists
  if(!dir.exists(target_dir)) dir.create(target_dir)
  
  if(dir.exists(target_dir)){
    #check existing directories
    existing_dir = dir(target_dir)
    
    #create directories based on partition key if they don't exist
    unique(file_list$partition_key) %>%
      str_subset(existing_dir,negate = TRUE) %>%
      map(.,~dir.create(paste0(target_dir,.)))
  }
  
}


#download data, read in files and write them as parquet
download_data <- function(target_dir,file_list){
  
  require(tools)
  require(progress)
  require(stringr)
  require(dplyr)
  require(purrr)
  require(data.table)
  require(arrow)
  
  file_num = nrow(file_list)
  
  pb <- progress_bar$new(total = file_num,
                         format = "  [:bar] :percent :eta",
                         clear = FALSE)
  
  #loop through undownloaded files and download them
  for(i in 1:file_num){
    pb$tick()
    target_name_path <- paste0(target_dir,files_to_download$partition_key[i],"/",files_to_download$file_name[i])
    match_name <- file_path_sans_ext(file_path_sans_ext(basename(target_name_path))) #removing ext twice because csv.gz
    
    #Download file if it doesn't exist on the drive
    if(is_empty(list.files(target_dir,pattern = match_name,recursive = TRUE))){
      download.file(url=files_to_download$link[i],
                    destfile = target_name_path,
                    quiet = TRUE)
    }
    
    if(str_detect(list.files(target_dir,pattern = match_name,recursive = TRUE),".csv.gz")){
      temp_df <- fread(target_name_path,keepLeadingZeros = T)
      write_parquet(temp_df,str_replace(target_name_path,".csv.gz",".parquet"))
      file.remove(target_name_path)
    }
    
    
  }
}

#function to download files using curl
bdown=function(url, file, access_token){
  library(RCurl)
  send_headers = c('accept' = 'application/json','Authorization' = paste0('Bearer ',access_token))
  
  f = CFILE(file, mode="wb")
  a = curlPerform(url = url, writedata = f@ref, noprogress=FALSE,httpheader=send_headers)
  close(f)
  return(a)
}
