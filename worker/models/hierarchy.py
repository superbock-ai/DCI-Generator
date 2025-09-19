"""
Hierarchy models for the DCI Generator taxonomy structure
"""

from typing import Dict, List, Optional
from pydantic import BaseModel

from .analysis import AnalysisResult


class TaxonomyItem(BaseModel):
    """Represents a taxonomy item with its relationship ID"""
    taxonomy_relationship_id: str
    taxonomy_item_id: str
    name: str
    category: str  # segment_type, benefit_type, limit_type, condition_type, exclusion_type
    description: str
    aliases: List[str]
    examples: List[str]
    llm_instruction: str = ""
    unit: str = ""
    data_type: str = ""
    
    @classmethod
    def create_from_graphql(cls, taxonomy_relationship_id: str, item: Dict) -> 'TaxonomyItem':
        """Create TaxonomyItem from GraphQL response, handling None values"""
        return cls(
            taxonomy_relationship_id=taxonomy_relationship_id,
            taxonomy_item_id=item['id'],
            name=item['taxonomy_item_name'],
            category=item['category'],
            description=item.get('description', ''),
            aliases=item.get('aliases', []),
            examples=item.get('examples', []),
            llm_instruction=item.get('llm_instruction', ''),
            unit=item.get('unit') or '',  # Handle None values
            data_type=item.get('data_type') or '',  # Handle None values
        )


class HierarchyNode:
    """Represents a node in the taxonomy hierarchy"""
    def __init__(self, taxonomy_item: TaxonomyItem, parent: Optional['HierarchyNode'] = None):
        self.taxonomy_item = taxonomy_item
        self.parent = parent
        self.children: List['HierarchyNode'] = []
        self.analysis_result: Optional[AnalysisResult] = None
    
    def add_child(self, child: 'HierarchyNode'):
        child.parent = self
        self.children.append(child)
    
    def get_hierarchy_context(self) -> str:
        """Build detailed context string showing the full hierarchy path with section references"""
        path = []
        current = self
        while current:
            if current.analysis_result and current.analysis_result.is_included:
                item_info = f"{current.taxonomy_item.name}: {current.analysis_result.llm_summary}"
                if current.analysis_result.section_reference:
                    item_info += f" (Fundstelle: {current.analysis_result.section_reference})"
                path.append(item_info)
            current = current.parent
        
        if not path:
            return ""
        
        path.reverse()  # Start from root
        context = "**ZU ANALYSIERENDER HIERARCHIEKONTEXT:**\n"
        for i, item in enumerate(path):
            indent = "  " * i
            context += f"{indent}- {item}\n"
        
        return context