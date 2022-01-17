CREATE EXTERNAL TABLE `campaign_metrics`(
   `auctionId` bigint,
   `eventTimestamp` bigint,
   `publisher` string,
   `application` string,
   `country` string,
   `userId` string,
   `campaignId` bigint,
   `event` string)
 PARTITIONED BY (
   `year` int,
   `month` int,
   `date` int)
 CLUSTERED BY (
   auctionId)
 INTO 16 BUCKETS
 STORED AS PARQUET
 LOCATION
   'hdfs://data/campaignMetrics'