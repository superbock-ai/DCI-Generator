#!/usr/bin/env python3
"""
Quick script to show the exact JSON schema that will be sent to OpenAI
"""

import sys
import os
import json

# Add worker directory to path
sys.path.insert(0, 'worker')

from simple_worker import AnalysisResult

# Generate the schema
schema = AnalysisResult.model_json_schema()

print("=== EXACT JSON SCHEMA SENT TO OPENAI ===")
print(json.dumps(schema, indent=2))

print("\n=== FULL OPENAI REQUEST FORMAT ===")
openai_format = {
    "response_format": {
        "type": "json_schema",
        "json_schema": {
            "name": "analysis_result",
            "description": "Structured analysis result for insurance document processing",
            "schema": schema,
            "strict": True
        }
    }
}
print(json.dumps(openai_format, indent=2))