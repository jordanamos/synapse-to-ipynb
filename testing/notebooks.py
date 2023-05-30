from __future__ import annotations

import json

IPYNB = json.loads(
    r"""
{
"nbformat": 4,
"nbformat_minor": 2,
"metadata": {
    "saveOutput": true,
    "enableDebugMode": false,
    "kernelspec": {
        "name": "synapse_pyspark",
        "display_name": "python"
    },
    "language_info": {
        "name": "python"
    },
    "sessionKeepAliveTimeout": 30
},
"cells": [
    {
        "cell_type": "code",
        "execution_count": 1,
        "metadata": {},
        "outputs": [
            {
                "name": "stdout",
                "output_type": "stream",
                "text": [
                    "hello\n"
                ]
            }
        ],
        "source": [
            "import os\n",
            "print(\"hello\")"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": null,
        "metadata": {},
        "outputs": [],
        "source": []
    }
]
}
"""
)

SYNAPSE_NB = json.loads(
    r"""
{
	"name": "test_spark",
	"properties": {
		"nbformat": 4,
		"nbformat_minor": 2,
		"bigDataPool": {
			"referenceName": "spark33min",
			"type": "BigDataPoolReference"
		},
		"sessionProperties": {
			"driverMemory": "28g",
			"driverCores": 4,
			"executorMemory": "28g",
			"executorCores": 4,
			"numExecutors": 1,
			"conf": {
				"spark.dynamicAllocation.enabled": "false",
				"spark.dynamicAllocation.minExecutors": "1",
				"spark.dynamicAllocation.maxExecutors": "1",
				"spark.autotune.trackingId": "79d2ce32-538c-48ca-bd6a-fd9e2bd39ede"
			}
		},
		"metadata": {
			"saveOutput": true,
			"enableDebugMode": false,
			"kernelspec": {
				"name": "synapse_pyspark",
				"display_name": "Synapse PySpark"
			},
			"language_info": {
				"name": "python"
			},
			"a365ComputeOptions": {
				"id": "/subscriptions/81675d9c-8576-4995-87c8-f6468c86e9e1/resourceGroups/rg-cpru-edp-dev-ae/providers/Microsoft.Synapse/workspaces/synw-cpru-edp-dev-ae/bigDataPools/spark33min",
				"name": "spark33min",
				"type": "Spark",
				"endpoint": "https://synw-cpru-edp-dev-ae.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/spark33min",
				"auth": {
					"type": "AAD",
					"authResource": "https://dev.azuresynapse.net"
				},
				"sparkVersion": "3.2",
				"nodeCount": 3,
				"cores": 4,
				"memory": 28,
				"automaticScaleJobs": false
			},
			"sessionKeepAliveTimeout": 30
		},
		"cells": [
			{
				"cell_type": "markdown",
				"metadata": {
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# Parameters"
				]
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015\r\n",
					"source_name = \"qcivil\"\r\n",
					"dataset_iterator = \"[\\\"qcivil.aud_audit.json\\\"]\"\r\n",
					""
				],
				"execution_count": 94
			},
			{
				"cell_type": "markdown",
				"metadata": {
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# Imports"
				]
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"import pyspark.sql.functions as F\r\n",
					"import delta.tables as dt\r\n",
					"import json\r\n",
					"\r\n",
					"from pyspark.sql.utils import (\r\n",
					"    AnalysisException, \r\n",
					"    CapturedException, \r\n",
					"    IllegalArgumentException, \r\n",
					"    ParseException, \r\n",
					"    PythonException, \r\n",
					"    QueryExecutionException, \r\n",
					"    StreamingQueryException, \r\n",
					"    UnknownException\r\n",
					")"
				],
				"execution_count": 95
			},
			{
				"cell_type": "markdown",
				"metadata": {
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# Dataset processing"
				]
			},
			{
				"cell_type": "markdown",
				"metadata": {
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"## Schema Stuff"
				]
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"from pyspark.sql.functions import col\r\n",
					"# Temporarily import all types until we settle on those that are required\r\n",
					"from pyspark.sql.types import *\r\n",
					"\r\n",
					"audit_schema = (\r\n",
					"    StructType()\r\n",
					"        .add('auditid', IntegerType(), False)\r\n",
					"        .add('aud_datetime', TimestampType(), False)\r\n",
					"        .add('userlogon', StringType(), False)\r\n",
					"        .add('tablename', StringType(), False)\r\n",
					"        .add('idcol', IntegerType(), True)\r\n",
					"        .add('timestamp', IntegerType(), True)\r\n",
					"        .add('sourcetimestamp', StringType(), True)\r\n",
					"        .add('activity', StringType(), True)\r\n",
					")\r\n",
					"\r\n",
					"staging_path = f\"abfss://staging@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/initial/{dataset_config['staging']['file_name']}\"\r\n",
					"staging_df = spark.read.parquet(staging_path)\r\n",
					"\r\n",
					"staging_df = (\r\n",
					"    staging_df\r\n",
					"        .withColumn(\"auditid\", col(\"auditid\").cast(IntegerType()))\r\n",
					"        .withColumn(\"aud_datetime\", col(\"aud_datetime\").cast(TimestampType()))\r\n",
					"        .withColumn(\"idcol\", col(\"idcol\").cast(IntegerType()))\r\n",
					"        .withColumn(\"timestamp\", col(\"idcol\").cast(IntegerType()))\r\n",
					")\r\n",
					"\r\n",
					"staging_df.printSchema()\r\n",
					"staging_df.show(5)"
				],
				"execution_count": 96
			},
			{
				"cell_type": "markdown",
				"metadata": {
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"## Initial"
				]
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# Parse the input parameter for looping\r\n",
					"parsed_iterator = json.loads(dataset_iterator)\r\n",
					"print(parsed_iterator)\r\n",
					"\r\n",
					"# config_path = f\"abfss://config@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/{parsed_iterator[0]}\"\r\n",
					"config_path = f\"abfss://config@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/{parsed_iterator[0]}\"\r\n",
					"print(f\"Config path: {config_path}\")\r\n",
					"dataset_config = json.loads(spark.read.option(\"multiline\", \"true\").json(config_path).toJSON().first())\r\n",
					"print(f\"Processing: {dataset_config['dataset_name']}...\")\r\n",
					"\r\n",
					"# Staging\r\n",
					"staging_path = f\"abfss://staging@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/initial/{dataset_config['staging']['file_name']}\"\r\n",
					"print(f\"Staging path: {staging_path}\")\r\n",
					"\r\n",
					"# staging_df = spark.read.parquet(staging_path)\r\n",
					"staging_df = (\r\n",
					"    staging_df\r\n",
					"        .withColumn(\"auditid\", col(\"auditid\").cast(IntegerType()))\r\n",
					"        .withColumn(\"aud_datetime\", col(\"aud_datetime\").cast(TimestampType()))\r\n",
					"        .withColumn(\"idcol\", col(\"idcol\").cast(IntegerType()))\r\n",
					"        .withColumn(\"timestamp\", col(\"idcol\").cast(IntegerType()))\r\n",
					")\r\n",
					"\r\n",
					"raw_path = f\"abfss://test-sam@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/{dataset_config['dataset_name']}_schema_condition/\"\r\n",
					"print(f\"Raw path: {raw_path}\")\r\n",
					"\r\n",
					"delta_table = (\r\n",
					"    dt.DeltaTable\r\n",
					"        .createIfNotExists(spark)\r\n",
					"        .location(raw_path)\r\n",
					"        .addColumns(staging_df.schema)\r\n",
					"        .execute()\r\n",
					")\r\n",
					"\r\n",
					"# # Construct a list of merge criteria to be used in dynamic string\r\n",
					"# # generation supporting both single and composite primary keys\r\n",
					"primary_keys = dataset_config['config']['primary_key']   \r\n",
					"merge_condition = \" AND \".join([f\"delta.{pk} = new.{pk}\" for pk in primary_keys])\r\n",
					"\r\n",
					"# # Merge data in to table (this can be an initial or incremental load)\r\n",
					"(\r\n",
					"    delta_table\r\n",
					"        .alias(\"delta\")\r\n",
					"        .merge(\r\n",
					"            source = staging_df.alias(\"new\"),\r\n",
					"            condition = merge_condition\r\n",
					"        )\r\n",
					"        .whenMatchedUpdateAll()\r\n",
					"        .whenNotMatchedInsertAll()\r\n",
					"        .execute()\r\n",
					")\r\n",
					""
				],
				"execution_count": 106
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"spark.sql(f\"CREATE TABLE aud_audit_schema_condition USING DELTA LOCATION 'abfss://test-sam@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/{dataset_config['dataset_name']}_schema_condition/'\")"
				],
				"execution_count": 103
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					},
					"microsoft": {
						"language": "sparksql"
					},
					"collapsed": false
				},
				"source": [
					"%%sql\r\n",
					"SELECT count(*) \r\n",
					"FROM aud_audit_schema_condition"
				],
				"execution_count": 107
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"spark.sql(\"OPTIMIZE aud_audit_schema_condition ZORDER BY (auditid)\")"
				],
				"execution_count": 108
			},
			{
				"cell_type": "markdown",
				"metadata": {
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"## Incremental"
				]
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# Parse the input parameter for looping\r\n",
					"parsed_iterator = json.loads(dataset_iterator)\r\n",
					"print(parsed_iterator)\r\n",
					"\r\n",
					"# config_path = f\"abfss://config@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/{parsed_iterator[0]}\"\r\n",
					"config_path = f\"abfss://config@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/{parsed_iterator[0]}\"\r\n",
					"print(f\"Config path: {config_path}\")\r\n",
					"dataset_config = json.loads(spark.read.option(\"multiline\", \"true\").json(config_path).toJSON().first())\r\n",
					"print(f\"Processing: {dataset_config['dataset_name']}...\")\r\n",
					"\r\n",
					"# Staging\r\n",
					"staging_path = f\"abfss://staging@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/incremental/{dataset_config['staging']['file_name']}\"\r\n",
					"print(f\"Staging path: {staging_path}\")\r\n",
					"\r\n",
					"# staging_df = spark.read.parquet(staging_path)\r\n",
					"staging_df = (\r\n",
					"    staging_df\r\n",
					"        .withColumn(\"auditid\", col(\"auditid\").cast(IntegerType()))\r\n",
					"        .withColumn(\"aud_datetime\", col(\"aud_datetime\").cast(TimestampType()))\r\n",
					"        .withColumn(\"idcol\", col(\"idcol\").cast(IntegerType()))\r\n",
					"        .withColumn(\"timestamp\", col(\"idcol\").cast(IntegerType()))\r\n",
					")\r\n",
					"\r\n",
					"raw_path = f\"abfss://test-sam@dlscpruedpdatadevae.dfs.core.windows.net/{source_name}/{dataset_config['dataset_name']}_schema_condition/\"\r\n",
					"print(f\"Raw path: {raw_path}\")\r\n",
					"\r\n",
					"delta_table = (\r\n",
					"    dt.DeltaTable\r\n",
					"        .createIfNotExists(spark)\r\n",
					"        .location(raw_path)\r\n",
					"        .addColumns(staging_df.schema)\r\n",
					"        .execute()\r\n",
					")\r\n",
					"\r\n",
					"# # Construct a list of merge criteria to be used in dynamic string\r\n",
					"# # generation supporting both single and composite primary keys\r\n",
					"primary_keys = dataset_config['config']['primary_key']   \r\n",
					"merge_condition = \" AND \".join([f\"delta.{pk} = new.{pk}\" for pk in primary_keys])\r\n",
					"\r\n",
					"# # Merge data in to table (this can be an initial or incremental load)\r\n",
					"(\r\n",
					"    delta_table\r\n",
					"        .alias(\"delta\")\r\n",
					"        .merge(\r\n",
					"            source = staging_df.alias(\"new\"),\r\n",
					"            condition = merge_condition\r\n",
					"        )\r\n",
					"        .whenMatchedUpdateAll()\r\n",
					"        .whenNotMatchedInsertAll()\r\n",
					"        .execute()\r\n",
					")"
				],
				"execution_count": 111
			},
			{
				"cell_type": "markdown",
				"metadata": {
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# Tests"
				]
			},
			{
				"cell_type": "code",
				"metadata": {
					"jupyter": {
						"source_hidden": false,
						"outputs_hidden": false
					},
					"nteract": {
						"transient": {
							"deleting": false
						}
					}
				},
				"source": [
					"# BASIC - 4 cores, 1 executor\r\n",
					"# aud_audit\r\n",
					"# initial\r\n",
					"# table creation -  5 sec 415 ms\r\n",
					"# load data - 2 min 15 sec 699 m\r\n",
					"# load data with schema - 1 min 39 sec 683 ms\r\n",
					"# load data with schema and auditid condition - 1 min 54 sec 369 ms\r\n",
					"\r\n",
					"# incremental\r\n",
					"# load data - 2 min 25 sec 449 ms\r\n",
					"# load data with schema - 3 min 5 sec 337 ms\r\n",
					"# load data with schema and auditid conditiond - 3 min 44 sec 248 ms"
				],
				"execution_count": 101
			}
		]
	}
}
"""
)
