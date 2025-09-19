"""
Analysis result models for the DCI Generator
"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator


class AnalysisResult(BaseModel):
    """Strict analysis result model with numeric value validation"""
    item_name: str = Field(..., description="Name of the analyzed item")
    is_included: bool = Field(..., description="Whether the item is included in the insurance coverage")
    section_reference: str = Field(..., description="Document section reference where the item was found")
    full_text_part: str = Field(..., description="Full text excerpt from the document")
    llm_summary: str = Field(..., description="LLM-generated summary of the analysis")
    description: str = Field(..., description="Description of what the item covers")
    unit: str = Field(..., description="Unit of measurement (e.g., CHF, days, percent)")
    value: Optional[float] = Field(None, description="Numeric value as number - clean numeric value only (e.g., 50000, 1500) or null if no value")
    unlimited: bool = Field(False, description="Whether this item has unlimited coverage (true for 'unbegrenzt', 'unlimited', etc.)")
    input_prompt: str = Field(..., description="The input prompt used for this analysis")
    
    @field_validator('value', mode='before')
    @classmethod
    def validate_value(cls, v):
        """Convert empty strings and non-numeric values to None"""
        if v == '' or v == 'N/A' or v == '0.0':
            return None
        if isinstance(v, str):
            # Try to parse Swiss number format
            cleaned = v.replace("'", "").replace(",", "")
            try:
                return float(cleaned)
            except (ValueError, TypeError):
                return None
        return v
    
    # Clean implementation - native LangChain structured output handles schema automatically
    
    class Config:
        """Pydantic configuration for strict validation"""
        json_schema_extra = {
            "additionalProperties": False,
            "examples": [
                {
                    "item_name": "coverage_limit",
                    "is_included": True,
                    "section_reference": "Section A.1",
                    "full_text_part": "Maximum coverage of 50'000 CHF per event",
                    "llm_summary": "Coverage limit found with maximum amount specified",
                    "description": "Maximum coverage amount per insurance event", 
                    "unit": "CHF",
                    "value": 50000,
                    "unlimited": False,
                    "input_prompt": "..."
                },
                {
                    "item_name": "medical_assistance",
                    "is_included": True,
                    "section_reference": "Section F.3",
                    "full_text_part": "Medical assistance coverage is unlimited",
                    "llm_summary": "Unlimited medical assistance coverage found",
                    "description": "Unlimited medical assistance coverage", 
                    "unit": "N/A",
                    "value": None,
                    "unlimited": True,
                    "input_prompt": "..."
                }
            ]
        }  # Store the entire input prompt used for analysis