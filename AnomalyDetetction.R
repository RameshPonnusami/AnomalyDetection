		library(tibbletime);
		library(tidyverse)
		library(AnomalyDetection);
		library(plyr);
		library(data.table);
		library(RMariaDB);



		dbConnection <- dbConnect(MariaDB(),user='root', password='password', dbname='r', host='127.0.0.1')
		filterZoneQuery <- 'select created_time,load_mwh,zone from tablename where zone= ? ';
		resultSet <- dbSendQuery(dbConnection, filterZoneQuery, list('Central'));
		dataSource <- dbFetch(resultSet);
		dataSource<- group_by(dataSource,zone);
		dataSource<- tbl_df(dataSource);
		Sys.setenv(TZ = 'UTC')
		dataSource$created_time<- as.POSIXct(dataSource$created_time,format='%Y-%m-%d %H:%M:%S', tz='UTC');
		formattedData <- as_tbl_time(dataSource, index = created_time);
		formattedData<-na.omit(formattedData)
		anomalizedResult = AnomalyDetectionVec(formattedData[,2], period=7, direction='both',longterm_period=30, plot=F,e_value = T)
		dev.off()
		anomalizedResult<- data.frame(anomalizedResult$anoms);
		dataSource$index <- seq.int(nrow(dataSource));
		anomalizedList <- data.table(anomalizedResult, key = 'index'); 
		dataSourceList <- data.table(dataSource, key = 'index');
		identifiedAnomaly<-join(anomalizedList, dataSourceList,type = 'inner');

		identifiedAnomaly<-cbind(identifiedAnomaly, process_id=233)

		insertAnomalyQuery <- 'INSERT INTO `identified_anomaly_all_zone_by_detection`(`index_id`, `anomaly`, `process_id`, `timezone`, `timestamp`, `expectedValue`) VALUES (?,?,?,?,?,?)';
		res<-dbExecute(dbConnection, insertAnomalyQuery, list(identifiedAnomaly$index,identifiedAnomaly$load_mwh,identifiedAnomaly$process_id, identifiedAnomaly$created_time,identifiedAnomaly$zone,identifiedAnomaly$expected_value));
		dbDisconnect(dbConnection);
