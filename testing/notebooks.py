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
    "name": "Notebook 3",
    "properties": {
        "folder": {
            "name": "gold/fact"
        },
        "nbformat": 4,
        "nbformat_minor": 2,
        "sessionProperties": {
            "driverMemory": "28g",
            "driverCores": 4,
            "executorMemory": "28g",
            "executorCores": 4,
            "numExecutors": 2,
            "conf": {
                "spark.dynamicAllocation.enabled": "false",
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "2"
            }
        },
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
                "execution_count": null,
                "metadata": {},
                "outputs": [],
                "source": [
                    "\"\"\"A notebook for converting data from bronze to silver.\"\"\""
                ]
            }
        ]
    }
}
"""
)

SYNAPSE_NB_NO_FOLDER = json.loads(
    r"""
{
    "name": "Notebook 4",
    "properties": {
        "nbformat": 4,
        "nbformat_minor": 2,
        "sessionProperties": {
            "driverMemory": "28g",
            "driverCores": 4,
            "executorMemory": "28g",
            "executorCores": 4,
            "numExecutors": 2,
            "conf": {
                "spark.dynamicAllocation.enabled": "false",
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "2"
            }
        },
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
                "execution_count": null,
                "metadata": {},
                "outputs": [],
                "source": [
                    "\"\"\"A notebook for converting data from bronze to silver.\"\"\""
                ]
            }
        ]
    }
}
"""
)
