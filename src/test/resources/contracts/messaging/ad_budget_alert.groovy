package contracts.messaging

/*
 * Contract for AD_BUDGET_ALERT event emitted by WalletService.
 */
org.springframework.cloud.contract.spec.Contract.make {
    description("Should publish AD_BUDGET_ALERT to ad-events")
    label("ad_budget_alert")
    input { triggeredBy('fireBudgetAlertEvent()') }
    outputMessage {
        sentTo('campaign-alerts')
        headers {
            header('eventType', 'AD_BUDGET_ALERT')
            header('aggregateType', 'WALLET')
        }
        body([
            campaignId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            advertiserId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            walletId: $(producer(regex('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'))),
            balance: "0.00"
        ])
    }
}
