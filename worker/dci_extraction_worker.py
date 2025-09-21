"""
Simplified DocumentAnalyzer - Process nested hierarchy with 15 concurrent requests
"""

import asyncio
import json
from typing import Dict, List, Any
# Clean imports - no backwards compatibility cruft
from prompt_manager import PromptManager
from models import AnalysisResult, TaxonomyItem, HierarchyNode
from utils import DebugManager, get_config, get_analysis_logger
from services import LLMService, GraphQLService


# AnalysisResult has been moved to models/analysis.py  # Store the entire input prompt used for analysis


# TaxonomyItem has been moved to models/hierarchy.py


# HierarchyNode has been moved to models/hierarchy.py


# get_debug_filename has been moved to utils/debug.py


# save_debug_results has been moved to utils/debug.py


# append_debug_result has been moved to utils/debug.py


# load_debug_results has been moved to utils/debug.py

# load_debug_results_with_categories has been moved to utils/debug.py


class SimplifiedDocumentAnalyzer:
    """
    Compatibility facade for the clean DocumentAnalyzer architecture
    
    This class maintains the existing API while delegating to the new clean implementation.
    All the messy legacy methods have been replaced with clean, focused components.
    """
    
    def __init__(self, dcm_id: str, product_id: str = None):
        # Import here to avoid circular imports
        from analyzers import DocumentAnalyzer
        
        # Delegate to the clean implementation
        self._analyzer = DocumentAnalyzer(dcm_id, product_id)
        
        # Expose properties for backwards API compatibility
        self.dcm_id = dcm_id
        self.product_id = product_id
        self.debug_enabled = False
        self.hierarchy_nodes = self._analyzer.hierarchy_nodes
        self.config = self._analyzer.config
        self.logger = self._analyzer.logger
        self.semaphore = self._analyzer.semaphore
        self.prompt_manager = self._analyzer.prompt_manager
        
        # Delegate services for compatibility
        self.llm_service = self._analyzer.llm_service
        self.graphql_service = self._analyzer.graphql_service
        self.debug_manager = self._analyzer.debug_manager
    
    def fetch_taxonomy_data(self) -> None:
        """Fetch complete taxonomy hierarchy - delegates to clean implementation"""
        self._analyzer.load_taxonomy()
        # Update reference for compatibility
        self.hierarchy_nodes = self._analyzer.hierarchy_nodes
    
    async def analyze_document(self, document_text: str, debug: bool = False) -> Dict[str, Any]:
        """Main analysis function - delegates to clean implementation"""
        self.debug_enabled = debug
        result = await self._analyzer.analyze_document(document_text, debug)
        # Update reference for compatibility
        self.hierarchy_nodes = self._analyzer.hierarchy_nodes
        return result
    
    def get_hierarchical_results_for_seeding(self) -> Dict[str, Any]:
        """Get results for seeding - delegates to clean implementation"""
        result = self._analyzer.get_hierarchical_results_for_seeding()
        # Update reference for compatibility
        self.hierarchy_nodes = self._analyzer.hierarchy_nodes
        return result
    
    # Legacy method stubs - these are no longer needed but kept for compatibility
    def _count_nodes(self, node):
        """Legacy method - functionality moved to clean implementation"""
        return 1 + sum(self._count_nodes(child) for child in node.children)
    
    def _count_modifiers_recursive(self, nodes):
        """Legacy method - functionality moved to clean implementation"""
        return self._analyzer.hierarchy_processor._count_modifiers_recursive(nodes)
    
    def _collect_debug_results_from_nodes(self, nodes):
        """Legacy method - functionality moved to clean implementation"""
        return {node.taxonomy_item.taxonomy_relationship_id: node.analysis_result 
                for node in self._analyzer._get_all_nodes_with_results()}
    
    def _apply_debug_results_to_nodes(self, nodes, debug_results):
        """Legacy method - delegates to clean implementation"""
        self._analyzer._apply_debug_results_to_nodes(nodes, debug_results)
    
    def _collect_results_hierarchical(self, nodes):
        """Legacy method - delegates to clean implementation"""
        return self._analyzer.results_collector.collect_hierarchical_results(nodes)
    
    def _collect_children_hierarchical(self, nodes, parent_data):
        """Legacy method - delegates to clean implementation"""
        return self._analyzer.results_collector._collect_children_hierarchical(nodes, parent_data)
    
    def _collect_results_recursive(self, nodes, results):
        """Legacy method - delegates to clean implementation"""
        flat_results = self._analyzer.results_collector.collect_flat_results(nodes)
        results.update(flat_results)


# Simple test function
async def main():
    """Test the simplified analyzer"""
    analyzer = SimplifiedDocumentAnalyzer(dcm_id="test", product_id="test-product")
    analyzer.fetch_taxonomy_data()
    
    # Test with a simple document
    test_document = "This is a test insurance document..."
    results = await analyzer.analyze_document(test_document, debug=True)
    
    # Results output handled by caller


if __name__ == "__main__":
    asyncio.run(main())