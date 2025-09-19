"""
Analyzers package for the DCI Generator

This package provides clean, focused analyzer classes with proper separation of concerns:
- DocumentAnalyzer: Main orchestrator for document analysis
- HierarchyProcessor: Handles hierarchy traversal and node analysis
- ResultsCollector: Manages result collection and formatting
"""

from .document import DocumentAnalyzer, HierarchyProcessor, ResultsCollector

__all__ = [
    'DocumentAnalyzer',
    'HierarchyProcessor', 
    'ResultsCollector'
]