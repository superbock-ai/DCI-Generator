"""
Models package for the DCI Generator

This package contains all data models used throughout the system:
- AnalysisResult: Core analysis result model with OpenAI structured output support
- TaxonomyItem: Represents items in the insurance taxonomy hierarchy  
- HierarchyNode: Tree node for building and traversing the taxonomy hierarchy
"""

from .analysis import AnalysisResult
from .hierarchy import TaxonomyItem, HierarchyNode

__all__ = [
    'AnalysisResult',
    'TaxonomyItem', 
    'HierarchyNode'
]