# Databricks notebook source
marketplaceLink = 'https://e2-dogfood.staging.cloud.databricks.com/marketplace/consumer/listings/8e19a0f3-5cca-41e5-839c-229f4e39266d?o=6051921418418893'
marketplaceProductName = 'Enterprise Software Sales Dataset'

sharedCatalogName = 'amitabh_arora_enterprise_software_sales_dataset'
sharedSchemaName ='enterprise_software_sales_sample'
catalogName = 'amitabh_arora_catalog'
schemaName ='enterprise_software_sales_sample_views'

# COMMAND ----------

# Install the catalog from the marketplace
%pip install databricks-multicloud

from databricks_multicloud import MarketplaceClient

# Initialize the Marketplace client
marketplace_client = MarketplaceClient()

# Install the catalog
marketplace_client.install_catalog(
    marketplace_link=marketplaceLink,
    catalog_name=sharedCatalogName,
    schema_name=sharedSchemaName
)
