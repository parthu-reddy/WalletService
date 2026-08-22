package contracts.messaging

/*
 * Contract for AD_BUDGET_ALERT emitted by WalletService.
 *
 * WalletService.publishBudgetAlert writes an outbox event with aggregateType ADVERTISEMENT,
 * which OutboxProcessor routes to the ad-events topic -- the same topic CampaignService's
 * CampaignAlertConsumer listens on. The payload carries advertiserId, campaignId and eventId; CampaignAlertConsumer
 * derives its idempotency key from eventId.
 */
org.springframework.cloud.contract.spec.Contract.make {
    description("Should publish AD_BUDGET_ALERT to ad-events")
    label("ad_budget_alert")
    input { triggeredBy('fireBudgetAlertEvent()') }
    outputMessage {
        sentTo('ad-events')
        headers {
            header('eventType', 'AD_BUDGET_ALERT')
            header('aggregateType', 'ADVERTISEMENT')
        }
        body([
            advertiserId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            campaignId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            eventId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')))
        ])
    }
}
