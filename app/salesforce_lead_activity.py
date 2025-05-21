from salesforce_extractor import SalesforceLeadActivity
import typer

app = typer.Typer()

@app.command()
def bulk():
    """
    Extract all lead activity data in bulk mode.
    This will replace any existing data in the database.
    """
    lead_activity = SalesforceLeadActivity()
    lead_activity.bulk_extract()

@app.command()
def incremental():
    """
    Extract only new lead activity data in incremental mode.
    This will append only new records to the database.
    """
    lead_activity = SalesforceLeadActivity(mode="incremental")
    lead_activity.incremental_extract()


if __name__ == "__main__":
    app()