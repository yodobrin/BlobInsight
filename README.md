# BlobInsight

The main goal of this page is to outline the process or steps required to develop set of blob life cycle policies which are based on actual usage patterns.

| Attribute | Google Cloud Storage | Amazon S3 | Azure Blob Storage |
|-----------|----------------------|-----------|--------------------|
| Size | No | Yes | No |
| Last Modified Date | Yes | Yes | Yes |
| Last Accessed Date | Yes (via Autoclass) | No | Yes (with last access time tracking enabled) |
| Object Age | Yes | Yes | Yes |
| Storage Class | Yes  | Yes | Yes |
| Object Prefix | Yes | Yes | Yes |
| Object Tags | No | Yes | Yes (via blob index tags) |
| Versioning | No | Yes | Yes |
| Automatic Transitioning | Yes - Autoclass | No | Yes - Blob life cycle Policies |

The [gcp Auto class]([Title](https://cloud.google.com/storage/docs/autoclass)) allows customers that are uncertain of the access patterns to leverage the auto-class, this will not work for all use cases.

The __key differentiator__ in my opinion is from [aws]([Title](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)) as it allows for rules based on size.

## Abstract

Azure customers with very high storage bills, will benefit by applying blob life cycle policies to their storage accounts. Our current policies can address rudimentary scenarios for example: delete or move a blob after a certain number of days since it was created or accessed. However, these policies are not based on actual usage patterns, and therefore are not optimal. Size for example is never considered.

It is important to consider that the personas creating and managing lifecycle management policies are often not familiar with access patterns and need efficient and straightforward ways to govern and manage the blobs and optimize the storage billing.

## Data points

Finding usage patterns requires more than one data point. The inventory only shows a snap shot of the current state. The content of the inventory is not enough to determine usage patterns. Diagnostic logs provide detailed timeline of all requests made to storage accounts. Since the logs does not contain the meta-data in the inventory, we would need both data points. The inventory and the logs.

To save on cost, and since one cannot join inventory in log analytics, it is recommended to use archiving to storage accounts from the diagnostic settings. This will allow us to keep the logs for a longer period of time, and still be able to query them.

## Data collection

The following diagram illustrates the high level data flow for the inventory and the logs.

![Data Flow](media/2023-07-20-10-40-01.png)

1. The storage that requires monitoring
2. Output of the blob inventory rules
3. Diagnostic logs (blob level)
4. A periodic process that would move both data points to a shared data lake
5. Existing ot designated data lake
6. Analytic process that would join the two data points and produce the usage patterns

### Blob Inventory

Blob Inventory rules can be setup via the portal or using specific REST calls. This [document](https://learn.microsoft.com/en-us/azure/storage/blobs/blob-inventory) describe the multiple options to setup rules. In very high level you can create two types of periodic rules, daily or weekly, and you can capture the inventory as parquet or csv. FOr the later research steps it would advisable to use the parquet format. While you can leave the schema as is, it is also recommended to log only required meta-data. This will result in smaller files.

Quick summary:

- Pick and choose required schema elements only
- Weekly occurrences
- Parquet format
- Specific designated container

### Diagnostic logs

Diagnostic logs (classic) can be configured via the portal, for more information please visit this [document](https://learn.microsoft.com/en-us/azure/storage/common/manage-storage-analytics-logs?tabs=azure-portal). We recommend focusing on the Read operation on storage, as we do get update stamp and creation date from the inventory. Note that logs will accumulate, and as of now they cannot be stored directly to your hierarchal namespace.

### Data movement

Assuming the inventory is saved as parquet, the diagnostic logs, are saved as json lines, in an append_block blob type. This prevents us from directly reading the content using Spark. Therefore we need to move the logs to an adls-gen2 account and while we do the move, we can also convert the json lines to parquet.

There are multiple options to do this:

- Using Azure Data Factory
- Azure Functions
- Python notebook
- Spark job

The sample implementation would use Azure Data Factory. Using a copy activity with ```json``` and ```parquet``` as sink. Copy all logs from the storage account to the data lake. This sample does not address recurrence analysis, if you require to perform these analysis on periodic basis, it would be a good practice to automate the logs copy and deletion from the source storage.

## Tools

Data Factory would be used to move both data points to the data lake.
Our current suggestion is to leverage Azure databricks for its ease of operational and keeping your spark cluster to the minimum.

## Conclusion

Each storage account will have its own usage patterns, and therefore the policies will be different. The process described in this document and with the provided Databricks notebook, will allow you to better understand the access patterns and create policies that are based on actual usage patterns. Both this readme and the notebook are provided as is and as a baseline for further analytics or other research.