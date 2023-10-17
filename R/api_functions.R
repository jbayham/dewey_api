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


api_list_files <- function(access_token=NULL){
  
  require(httr)
  require(jsonlite)


  check <- GET(url = "https://app.deweydata.io/external-api/v3/products/eb6e748a-0fdd-4bc7-9dd7-bbed0890948d/files",
               config = add_headers("X-API-KEY"= access_token,
                                    'accept'= 'application/json'))
  
  out_list <- fromJSON(rawToChar(check$content))
  
  return(out_list)
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
