# Databricks notebook source
sharedCatalogName = 'Amitabh_Arora_Databricks_Enterprise_Software_Sales_Dataset'
catalogName = 'amitabh_arora_catalog'
schemaName ='enterprise_software_sales_sample'
viewSchemaName ='enterprise_software_sales_sample_views'

#spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogName} COMMENT 'Sample view sharing catalog'");
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogName}.{viewSchemaName}");
spark.sql(f"use {catalogName}.{viewSchemaName}");

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view for the accounts table
# MAGIC create or replace view accounts
# MAGIC  (
# MAGIC  industry comment 'The industry of the account', 
# MAGIC  id comment 'The unique identifier of the account',
# MAGIC  name comment 'The name of the account',
# MAGIC  TYPE comment 'The type of account',
# MAGIC  region_hq__c comment 'The headquarters region of the account',
# MAGIC  region__c comment 'The region of the account',
# MAGIC  company_size_segment__c comment 'The size segment of the company (either SMB, MM, or ENT)',
# MAGIC  annualrevenue comment 'The annual revenue of the account'
# MAGIC   )
# MAGIC     COMMENT 'This table contains information about accounts. It includes data on the industry, name, type, region, company size segment, service start date, and annual revenue of each account.'
# MAGIC as (
# MAGIC   select 
# MAGIC     case 
# MAGIC         when industry is null then array('Major Banks', 'Major Pharmaceuticals', 'Real Estate Investment Trusts', 'Business Services', 'Industrial Machinery/Components', 'Telecommunications Equipment', 'Oil & Gas Production')[floor(rand()*7)]
# MAGIC         else industry 
# MAGIC     end as industry,
# MAGIC     id,
# MAGIC     name,
# MAGIC     TYPE,
# MAGIC     region_hq__c,
# MAGIC     region__c,
# MAGIC     company_size_segment__c,
# MAGIC     annualrevenue
# MAGIC   from
# MAGIC     `Amitabh_Arora_Databricks_Enterprise_Software_Sales_Dataset`.enterprise_software_sales_sample.accounts
# MAGIC   );
# MAGIC
# MAGIC -- Create a view for the opportunity table
# MAGIC create or replace view opportunity
# MAGIC  (
# MAGIC  id comment 'the unique ID identifier for an opportunity', 
# MAGIC  ownerid comment 'the sales person who owns this opportunity',
# MAGIC  stagename comment 'the current stage of the opportunity. Currently used values are: "1. Discovery", "2. Demo", "3. Validation", "4. Procure", "5. Closed Won", "X. Closed Lost".',
# MAGIC  accountid,
# MAGIC  type,
# MAGIC  name comment "the written name of this opportunity. Contains the account name and a tag if it's new business",
# MAGIC  forecastcategory,
# MAGIC  description,
# MAGIC  leadsource,
# MAGIC  closedate,
# MAGIC  createddate,
# MAGIC  probability comment "the probability this opportunity is successful. The values are already between 0 and 1.",
# MAGIC amount comment "the amount in dollars of the potential opportunity",
# MAGIC New_Expansion_Booking_Annual__c,
# MAGIC New_Expansion_Booking_Annual_Wtd__c,
# MAGIC New_Recurring_Bookings_Manual__c,
# MAGIC New_Recurring_Bookings__c,
# MAGIC business_type__c
# MAGIC   )
# MAGIC     COMMENT 'This table contains the most updated information for each specific business opportunity. Each opportunity has a unique id, the name of the company, an amount, the probability it goes through, an assigned owner, and more info.'
# MAGIC as (
# MAGIC   select 
# MAGIC   id, 
# MAGIC   ownerid,
# MAGIC   stagename,
# MAGIC   accountid,
# MAGIC   type,
# MAGIC   name,
# MAGIC   forecastcategory,
# MAGIC   description,
# MAGIC   leadsource,
# MAGIC   date_add(closedate, 180) as closedate,
# MAGIC   date_add(createddate, 180) as createddate,
# MAGIC   probability,
# MAGIC   amount,
# MAGIC   New_Expansion_Booking_Annual__c,
# MAGIC   New_Expansion_Booking_Annual_Wtd__c,
# MAGIC   New_Recurring_Bookings_Manual__c,
# MAGIC   New_Recurring_Bookings__c,
# MAGIC   business_type__c
# MAGIC   from
# MAGIC  `Amitabh_Arora_Databricks_Enterprise_Software_Sales_Dataset`.enterprise_software_sales_sample.opportunity
# MAGIC   );
# MAGIC
# MAGIC   -- Create a view for the opportunity history table
# MAGIC create or replace view opportunityhistory
# MAGIC  (
# MAGIC  id comment 'Unique identifier for the opportunity history record',
# MAGIC  opportunityid comment 'Unique identifier for the opportunity this update is for',
# MAGIC  probability comment 'The probability of the opportunity being won',
# MAGIC  amount comment 'The amount of the opportunity',
# MAGIC  createdbyid comment 'Unique identifier for the user who created the opportunity history record',
# MAGIC  stagename comment 'The stage of the opportunity at the time the history record was created',
# MAGIC  forecastcategory comment 'The forecast category of the opportunity at the time the history record was created',
# MAGIC  createddate,
# MAGIC  closedate
# MAGIC   )
# MAGIC     COMMENT 'The opportunityhistory table contains a log of all the updates made to each specific opportunity. For example, each one opportunity row in the opportunity table will have multiple rows in this opportunityhistory table corresponding to all the updates that opportunity has had. The log contains information has unique id field, the opportunityid it corresponds to, its probability, stagename, and more'
# MAGIC as (
# MAGIC   select 
# MAGIC    id ,
# MAGIC   opportunityid,
# MAGIC   probability,
# MAGIC   amount,
# MAGIC   createdbyid,
# MAGIC   stagename,
# MAGIC   forecastcategory,
# MAGIC   date_add(closedate, 180) as closedate,
# MAGIC   date_add(createddate, 180) as createddate
# MAGIC   from
# MAGIC  `Amitabh_Arora_Databricks_Enterprise_Software_Sales_Dataset`.enterprise_software_sales_sample.opportunityhistory
# MAGIC   );
# MAGIC
# MAGIC -- Create a view for the period table
# MAGIC create or replace view period
# MAGIC  (
# MAGIC  FISCALYEARSETTINGSID comment 'Unique identifier for the fiscal year settings', 
# MAGIC  ISFORECASTPERIOD comment 'Boolean indicating if the period is a forecast period',
# MAGIC  PERIODLABEL comment 'Label for the period',
# MAGIC  ENDDATE comment 'Timestamp of the end date of the period',
# MAGIC  QUARTERLABEL comment 'Label for the quarter',
# MAGIC  STARTDATE comment 'Timestamp of the start date of the period',
# MAGIC  FULLYQUALIFIEDLABEL comment 'Fully qualified label for the period. This contains how users would refer to this time period in regular language.',
# MAGIC  NUMBER comment 'Numeric identifier for the period',
# MAGIC  ID comment 'Unique identifier for the period',
# MAGIC  TYPE comment 'Type of period (e.g. fiscal, calendar)'
# MAGIC   )
# MAGIC   COMMENT "The 'period' table contains metadata about different time periods used in the dataset. It includes details such as the fiscal year, whether it's a forecast period, and the label for the period. This information can be used to filter and group data based on time periods, making it easier to analyze and compare data across different timeframes. The table can also be used to identify the type of period, such as a quarter or a fiscal year, and to determine the start and end dates of each period."
# MAGIC as (
# MAGIC   select 
# MAGIC   FISCALYEARSETTINGSID,
# MAGIC   ISFORECASTPERIOD,
# MAGIC   PERIODLABEL,
# MAGIC   ENDDATE,
# MAGIC   QUARTERLABEL,
# MAGIC   STARTDATE,
# MAGIC   FULLYQUALIFIEDLABEL,
# MAGIC   NUMBER,
# MAGIC   ID,
# MAGIC   TYPE
# MAGIC   from
# MAGIC  `Amitabh_Arora_Databricks_Enterprise_Software_Sales_Dataset`.enterprise_software_sales_sample.period
# MAGIC   );
# MAGIC
# MAGIC -- Create a view for the teams table
# MAGIC create or replace view teams
# MAGIC  (
# MAGIC  USER comment 'Name of a team member',
# MAGIC  REGION comment 'Region where the user is located',
# MAGIC  SEGMENT comment 'Segment the user belongs to',
# MAGIC  MANAGER comment "Name of the user's manager",
# MAGIC  TEAM comment 'Name of the team'
# MAGIC   )
# MAGIC   COMMENT 'This table contains information about teams within the business, including their assigned region, segment, manager, general manager, and team name. It also includes data on whether the team is currently active or not. The data in this table is used for tracking and managing teams within the organization.'
# MAGIC as (
# MAGIC   select 
# MAGIC    USER,
# MAGIC   REGION,
# MAGIC   SEGMENT,
# MAGIC   MANAGER,
# MAGIC   TEAM
# MAGIC   from
# MAGIC  `Amitabh_Arora_Databricks_Enterprise_Software_Sales_Dataset`.enterprise_software_sales_sample.teams
# MAGIC   );
# MAGIC
# MAGIC -- Create a view for the user table
# MAGIC create or replace view user
# MAGIC  (
# MAGIC  id comment 'Unique identifier for each user', 
# MAGIC  name comment 'Name of the user',
# MAGIC  title comment 'Job title of the user',
# MAGIC  managerid comment "Unique identifier for the user's manager",
# MAGIC  role__c comment 'Role of the user in the system',
# MAGIC  segment__c comment 'Segment the user belongs to',
# MAGIC  region__c comment 'Region the user belongs to'
# MAGIC   )
# MAGIC     COMMENT 'This table contains information about team members in the CRM. It includes their unique ID, name, title, manager ID, role, segment, region, division, department, and whether they are active. The table also includes timestamps for when the user was created and last modified. This data is important for tracking user activity and managing access to different parts of the system.'
# MAGIC as (
# MAGIC   select 
# MAGIC    id, 
# MAGIC   name,
# MAGIC   title,
# MAGIC   managerid,
# MAGIC   role__c,
# MAGIC   segment__c,
# MAGIC   region__c
# MAGIC   from
# MAGIC  `Amitabh_Arora_Databricks_Enterprise_Software_Sales_Dataset`.enterprise_software_sales_sample.user
# MAGIC   );

# COMMAND ----------

# Grant catalog, schema, and table permissions to all users in workspace
spark.sql(f"GRANT USAGE ON CATALOG {catalogName} TO `account users`");
spark.sql(f"GRANT USAGE ON SCHEMA {catalogName}.{schemaName} TO `account users`");
spark.sql(f"GRANT SELECT ON VIEW {catalogName}.{schemaName}.accounts TO `account users`");
spark.sql(f"GRANT SELECT ON VIEW {catalogName}.{schemaName}.opportunity TO `account users`");
spark.sql(f"GRANT SELECT ON VIEW {catalogName}.{schemaName}.opportunityhistory TO `account users`");
spark.sql(f"GRANT SELECT ON VIEW {catalogName}.{schemaName}.period TO `account users`");
spark.sql(f"GRANT SELECT ON VIEW {catalogName}.{schemaName}.teams TO `account users`");
spark.sql(f"GRANT SELECT ON VIEW {catalogName}.{schemaName}.user TO `account users`");

# COMMAND ----------


