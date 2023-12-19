

library(pacman)
p_load(tidyverse,arrow,data.table,tools,progress,furrr)

from_dir="spend"
to_dir="spend_parquet"
# from_dir="advan_month_patterns"
# to_dir="advan_month_patterns_parquet"
from_file_ext = ".csv.gz"
to_file_ext = ".parquet"

#Check if from dir is correct
dir.exists(target_dir)

#Create to_dir if one does not exist
if(!dir.exists(to_dir)) dir.create(to_dir)

converted_list <- list.files(to_dir,pattern = to_file_ext) %>%
  str_remove(to_file_ext) %>%
  enframe(value = "fid",name=NULL)

convert_list <- list.files(from_dir,pattern = from_file_ext) %>%
  str_remove(from_file_ext) %>%
  enframe(value = "fid",name=NULL) %>%
  anti_join(converted_list) %>%
  pull()

#check fields
system.time({
  fread(str_c(from_dir,"/",convert_list[1],from_file_ext),keepLeadingZeros = T,nrows = Inf,nThread = 1) %>%
    glimpse()
})


#pb <- progress_bar$new(total = length(convert_list))
plan(multisession(workers = 10))

future_walk(convert_list,
     function(x){
      #pb$tick()
      temp_df <- fread(str_c(from_dir,"/",x,from_file_ext),keepLeadingZeros = T) %>%
        select(placekey,parent_placekey,safegraph_brand_ids,brands,store_id,location_name,street_address,poi_cbg,naics_code,region,iso_country_code,category_tags,date_range_start,date_range_end,
               raw_visit_counts,raw_visitor_counts,contains("normalized"),median_dwell,distance_from_home,
               opened_on,closed_on,tracking_closed_since,wkt_area_sq_meters)
      
      write_parquet(temp_df,str_c(to_dir,"/",file_path_sans_ext(file_path_sans_ext(basename(x))),to_file_ext))
  },.progress = T)


