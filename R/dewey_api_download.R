#This script downloads data from the dewey.io API

library(pacman)
p_load(tidyverse,jsonlite,httr,lubridate,RCurl)

source("R/api_functions.R")

#Setting parameters
target_dir = "/DATA/restricted_data/" #the directory where files will be downloaded
target_prefix = "safegraph_sp_tran_panel"  #the unique file type identifier (e.g., "advan_mp_normalization")
file_ext = ".csv" #the file extension
base_url = "https://marketplace.deweydata.io"


#Get token
jb_access_token = Sys.getenv("DEWEY_API_KEY")

#Query full list of downloadable files - be patient
file_list <- api_list_files(access_token = jb_access_token)$download_links


download.file(url=file_list$link[1],
              destfile = paste0(target_dir,file_list$file_name[1]))



########################
if(!dir.exists(target_dir)) dir.create(target_dir,recursive = TRUE)

#Check for downloaded files to avoid redundancy
dld_list <- list.files(target_dir,pattern = file_ext) %>%
  str_remove(file_ext) %>%
  enframe(value = "fid",name=NULL)

#Filter subset of files you need
dl_list <- file_list %>%
  mutate(createdAt=as_datetime(createdAt)) %>%
  filter(str_detect(fid,target_prefix)) %>%
  anti_join(dld_list)
  
#########################
#Download the files

#df=dl_list[1,] #for testing
#Loop through files and download
dl_list %>%
  select(fid,url) %>%
  group_split(fid) %>%
  walk(function(df){
    bdown(file = paste0(target_dir,"/",df$fid,file_ext),
          url = paste0(base_url,df$url),
          access_token = jb_access_token)
  })



