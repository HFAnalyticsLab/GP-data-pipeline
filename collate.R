## Collate data from s3
source('setup.R') # This contains s3 locations: p_bucket

library(tidyverse)
library(aws.s3)
library(data.table)

get_bucket(p_bucket, ## get appointment file paths
           prefix = 'GP_data/jan_23/app') %>%
  rbindlist() %>%
  select(Key) -> app_files

app_data <- lapply( ## read into a list
  app_files$Key,
  function(f) {
    aws.s3::s3read_using(
      FUN = vroom::vroom,
      object = f,
      bucket = p_bucket
    ) 
  }
) %>% 
  rbindlist()

get_bucket(p_bucket, 
           prefix = 'GP_data/jan_23/work') %>%
  rbindlist() %>%
  select(Key) -> work_files

## use most recent file 
work_data <- lapply( ## read into a list
  work_files$Key,
  function(f) {
    aws.s3::s3read_using(
      FUN = vroom::vroom,
      object = f,
      bucket = p_bucket
    ) 
  }
) %>% 
  rbindlist()

## Add cleaning / filtering here


