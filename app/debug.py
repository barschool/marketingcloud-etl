from salesforce_extractor import SalesforceLeadActivity


def bulk():
    lead_activity = SalesforceLeadActivity()
    lead_activity.bulk_extract()


def incremental():
    lead_activity = SalesforceLeadActivity(mode="incremental")
    lead_activity.incremental_extract()


incremental()