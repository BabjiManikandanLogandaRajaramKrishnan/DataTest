package com.s4m.datatest.entity

case class CampaignMetrics(auctionId: Long, contextTimestamp: Long, publisher: String, application: String,
                           country: String, userId: String, campaignId: Long, eventTimestamp: Long, event: String)
