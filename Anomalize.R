library(tidyverse);
library(anomalize);
library(tibbletime);
library(RMariaDB);

process_id<-233;
zone=;

dbConnection <- dbConnect(MariaDB(),user='root', password='password', dbname='rtios', host='127.0.0.1')
filterZoneQuery <- 'select created_time,load_mwh,zone from miso_hourly_load where zone= ? ';
resultSet <- dbSendQuery(dbConnection, filterZoneQuery, list('Central'));
dataSource <- dbFetch(resultSet);
dataSource<- group_by(dataSource,zone);
dataSource<- tbl_df(dataSource);
Sys.setenv(TZ = 'UTC')
dataSource$created_time<- as.POSIXct(dataSource$created_time,format='%Y-%m-%d %H:%M:%S', tz='UTC');
formattedData <- as_tbl_time(dataSource, index = created_time);
formattedData<-na.omit(formattedData)

anomalyResult<-formattedData%>% 
   time_decompose(load_mwh,method = 'twitter',frequency = 'auto',trend = 'auto',merge = TRUE,message = FALSE)%>%
   anomalize(remainder, method = 'gesd', alpha = 0.05) ;

anomalyResultWithProcess<-cbind(anomalyResult, process_id=process_id)
insertAnomalyQuery <- 'INSERT INTO  identified_anomaly_all_zone(`date`, `load_mwh`, `zone`, `anomaly_result`,process_id) values(?,?,?,?,?)';
res<-dbExecute(dbConnection, insertAnomalyQuery, list(anomalyResultWithProcess$created_time, anomalyResultWithProcess$load_mwh,anomalyResultWithProcess$zone,anomalyResultWithProcess$anomaly,anomalyResultWithProcess$process_id));
dbDisconnect(dbConnection);
