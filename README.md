# Mysql - SF Marketing Cloud 
Small integration script for extracting SFMC data into a local MySql db. 

Copy dedevcontainer.env.example to dedevcontainer.env and update with your credentials. 

### Docker
Image: docker.io/barschool/marketingcloud

python app/salesforce_lead_activity.py bulk
python app/salesforce_lead_activity.py incremental
