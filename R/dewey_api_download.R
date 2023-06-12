#This script downloads data from the dewey.io API

library(pacman)
p_load(tidyverse,jsonlite,httr,lubridate,RCurl)

source("R/api_functions.R")

#Setting parameters
target_dir = "/RSTOR/restricted_data/dewey/advan_month_patterns_stats" #the directory where files will be downloaded
target_prefix = "advan_mp_normalization"  #the unique file type identifier (e.g., "advan_mp_normalization")
file_ext = ".csv" #the file extension

#Get token
jb_access_token = get_token()

#Query full list of downloadable files - be patient
file_list <- api_list_files(access_token = jb_access_token,read_cache = FALSE)

########################
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
          url = paste0(base_url,df$url))
  })



