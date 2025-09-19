"""
Debug utilities for the DCI Generator
"""

import os
import json
from typing import Dict, Optional, Tuple
from models import AnalysisResult


class DebugManager:
    """Manages debug file operations and analysis result persistence"""
    
    def __init__(self, base_debug_dir: str = "debug"):
        """Initialize the debug manager with a base debug directory"""
        self.base_debug_dir = base_debug_dir
    
    def get_debug_filename(self, product_id: str, tier: str) -> str:
        """Generate debug filename for a specific tier."""
        return f"{self.base_debug_dir}/{product_id}_{tier}.debug.json"
    
    def save_debug_results(self, product_id: str, tier: str, results: Dict[str, AnalysisResult]):
        """Save analysis results to debug file with taxonomy_relationship_id as key."""
        filename = self.get_debug_filename(product_id, tier)
        
        # Ensure debug directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        debug_data = {
            "tier": tier,
            "product_id": product_id,
            "results": {}
        }
        
        # Convert results to dictionaries with taxonomy_relationship_id as key
        if results:
            for taxonomy_relationship_id, analysis_result in results.items():
                if hasattr(analysis_result, 'dict'):  # Pydantic model
                    debug_data["results"][taxonomy_relationship_id] = analysis_result.dict()
                else:
                    debug_data["results"][taxonomy_relationship_id] = analysis_result
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(debug_data, f, indent=2, ensure_ascii=False)
    
    def append_debug_result(self, product_id: str, tier: str, taxonomy_relationship_id: str, 
                          analysis_result: AnalysisResult, entity_category: str = None):
        """Append a single analysis result to the debug file incrementally."""
        if not product_id:  # Skip if no product_id available
            return
            
        filename = self.get_debug_filename(product_id, tier)
        
        # Ensure debug directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Load existing debug data or create new
        debug_data = {
            "tier": tier,
            "product_id": product_id,
            "results": {}
        }
        
        # Try to load existing file
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    debug_data = json.load(f)
            except Exception as e:
                # Continue with empty debug_data structure
                pass
        
        # Prepare the result data with entity category
        if hasattr(analysis_result, 'dict'):  # Pydantic model
            result_data = analysis_result.dict()
        else:
            result_data = analysis_result
        
        # Add entity category to the result data
        if entity_category:
            result_data["entity_category"] = entity_category
        
        # Add/update the new result
        debug_data["results"][taxonomy_relationship_id] = result_data
        
        # Save updated debug data
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(debug_data, f, indent=2, ensure_ascii=False)
    
    def load_debug_results(self, product_id: str, tier: str) -> Tuple[Optional[Dict[str, AnalysisResult]], bool]:
        """
        Load analysis results from debug file.
        Returns (results_dict, is_valid) tuple.
        """
        filename = self.get_debug_filename(product_id, tier)
        
        if not os.path.exists(filename):
            return None, False
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                debug_data = json.load(f)
            
            # Ensure debug_data has the expected structure
            if not isinstance(debug_data, dict) or "results" not in debug_data:
                print(f"❌ Invalid debug file structure: {filename}")
                return None, False
            
            # Handle empty results case
            if not debug_data["results"]:
                return {}, True
            
            # Convert back to AnalysisResult objects
            results = {}
            for taxonomy_relationship_id, result_dict in debug_data["results"].items():
                try:
                    results[taxonomy_relationship_id] = AnalysisResult(**result_dict)
                except Exception as e:
                    print(f"❌ Error loading debug result for {taxonomy_relationship_id}: {e}")
                    return None, False
            
            return results, True
            
        except Exception as e:
            print(f"❌ Error reading debug file {filename}: {e}")
            return None, False
    
    def load_debug_results_with_categories(self, product_id: str, tier: str) -> Tuple[Optional[Dict[str, AnalysisResult]], Optional[Dict[str, str]], bool]:
        """
        Load analysis results from debug file with entity categories.
        Returns (results_dict, categories_dict, is_valid) tuple.
        """
        filename = self.get_debug_filename(product_id, tier)
        
        if not os.path.exists(filename):
            return None, None, False
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                debug_data = json.load(f)
            
            # Ensure debug_data has the expected structure
            if not isinstance(debug_data, dict) or "results" not in debug_data:
                print(f"❌ Invalid debug file structure: {filename}")
                return None, None, False
            
            # Handle empty results case
            if not debug_data["results"]:
                return {}, {}, True
            
            # Convert back to AnalysisResult objects and extract categories
            results = {}
            categories = {}
            for taxonomy_relationship_id, result_dict in debug_data["results"].items():
                try:
                    # Extract entity category if present
                    entity_category = result_dict.pop("entity_category", None)
                    if entity_category:
                        categories[taxonomy_relationship_id] = entity_category
                    
                    results[taxonomy_relationship_id] = AnalysisResult(**result_dict)
                except Exception as e:
                    print(f"❌ Error loading debug result for {taxonomy_relationship_id}: {e}")
                    return None, None, False
            
            return results, categories, True
            
        except Exception as e:
            print(f"❌ Error reading debug file {filename}: {e}")
            return None, None, False


# Clean implementation - use DebugManager class directly, no global functions