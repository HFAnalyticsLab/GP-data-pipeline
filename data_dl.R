library(tidyverse)
library(rvest)
library(aws.s3)

source('setup.R') # This contains s3 locations: p_bucket

# Web locations
apps <- 'https://digital.nhs.uk/data-and-information/publications/statistical/appointments-in-general-practice/'
work <- 'https://digital.nhs.uk/data-and-information/publications/statistical/general-and-personal-medical-services/'

# function to get file locations
return_links <- function(web_data, search = '.zip'){
  
  read_html(web_data) %>% # read links from webpage
    html_nodes("a") %>%
    html_attr('href') -> links
  
  return(
    grep(search, links, value = TRUE)
  )
  
}

## most recent data
paste0(apps, 'january-2023') %>% 
  return_links(.) %>%
  grep('Regional', ., value = TRUE) %>%
  download.file(., destfile = 'app.zip', method = 'libcurl')

unzip('app.zip')
unlink('app.zip')
list.files() %>% grep('.csv', ., value = TRUE) -> to_move

for (i in to_move){
  
  put_object(file = i,
             object = paste0('GP_data/jan_23/app/', i),
             bucket = p_bucket,
             multipart = TRUE)
  
  unlink(i)
  
  }

paste0(work, '31-january-2023') %>%
  return_links(.) %>% ## function change
  grep('Practice', ., value = TRUE) %>%
  download.file(., destfile = 'work.zip', method = 'libcurl')

unzip('work.zip')
unlink('work.zip')
list.files() %>% grep('General Practice', ., value = TRUE) -> to_move

for (i in to_move){
  
  put_object(file = i,
             object = paste0('GP_data/jan_23/work/', i),
             bucket = p_bucket,
             multipart = TRUE)
  
  unlink(i)
  
}

