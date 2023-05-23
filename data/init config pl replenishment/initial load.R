library(readxl)
library(data.table)
library(dplyr)
library(AzureStor)

# write and process
config_pl <- read_excel("data/init config pl replenishment/config - PL.xlsx")
config_pl <- config_pl %>% 
  dplyr::select(PRODUCT_ID, NAME, REPLENISHMENT_DAYS, IS_DEFINED) %>% 
  dplyr::mutate(
    PRODUCT_ID = trimws(PRODUCT_ID)
  ) %>% 
  as.data.frame()

# store in csv
write.table(
  x = config_pl, 
  file = "C:/Users/peter.tomko/OneDrive - Dr. Max BDC, s.r.o/Plocha/ds_projects/DrMax-Group-Replenishment/data/init config pl replenishment/config_pl.csv", 
  sep = ",",
  quote=TRUE,
  row.names = FALSE, fileEncoding = "utf-8")
