"""
Simplified DocumentAnalyzer - Process nested hierarchy with 15 concurrent requests
"""

import os
import asyncio
import json
from typing import Dict, List, Any, Optional
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, field_validator
from prompt_manager import PromptManager


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
    
    @classmethod
    def model_json_schema(cls, by_alias=True, ref_template='#/$defs/{model}'):
        """Override to add additionalProperties: false and ensure all fields are required for OpenAI structured output"""
        schema = super().model_json_schema(by_alias=by_alias, ref_template=ref_template)
        schema['additionalProperties'] = False
        
        # OpenAI requires ALL properties to be in the required array
        if 'properties' in schema:
            schema['required'] = list(schema['properties'].keys())
        
        return schema
    
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


def get_debug_filename(product_id: str, tier: str) -> str:
    """Generate debug filename for a specific tier."""
    return f"debug/{product_id}_{tier}.debug.json"


def save_debug_results(product_id: str, tier: str, results: Dict[str, AnalysisResult]):
    """Save analysis results to debug file with taxonomy_relationship_id as key."""
    filename = get_debug_filename(product_id, tier)
    
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


def append_debug_result(product_id: str, tier: str, taxonomy_relationship_id: str, analysis_result: AnalysisResult, entity_category: str = None):
    """Append a single analysis result to the debug file incrementally."""
    if not product_id:  # Skip if no product_id available
        return
        
    filename = get_debug_filename(product_id, tier)
    
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


def load_debug_results(product_id: str, tier: str) -> tuple[Optional[Dict[str, AnalysisResult]], bool]:
    """
    Load analysis results from debug file.
    Returns (results_dict, is_valid) tuple.
    """
    filename = get_debug_filename(product_id, tier)
    
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

def load_debug_results_with_categories(product_id: str, tier: str) -> tuple[Optional[Dict[str, AnalysisResult]], Optional[Dict[str, str]], bool]:
    """
    Load analysis results from debug file with entity categories.
    Returns (results_dict, categories_dict, is_valid) tuple.
    """
    filename = get_debug_filename(product_id, tier)
    
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


class SimplifiedDocumentAnalyzer:
    """Simplified document analyzer processing nested hierarchy with 15 concurrent requests"""
    
    def __init__(self, dcm_id: str, product_id: str = None):
        self.dcm_id = dcm_id
        self.product_id = product_id  # Required for debug functionality
        self.debug_enabled = False  # Set by analyze_document method
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        # Initialize GraphQL client
        graphql_url = os.getenv("DIRECTUS_URL", "https://app-uat.quinsights.tech") + "/graphql"
        directus_token = os.getenv("DIRECTUS_AUTH_TOKEN")
        
        if not directus_token:
            raise ValueError("DIRECTUS_AUTH_TOKEN environment variable is required")
        
        transport = RequestsHTTPTransport(
            url=graphql_url,
            headers={"Authorization": f"Bearer {directus_token}"},
            use_json=True
        )
        self.graphql_client = Client(transport=transport, fetch_schema_from_transport=True)
        
        # Storage for hierarchy
        self.hierarchy_nodes: List[HierarchyNode] = []
        self.semaphore = asyncio.Semaphore(15)  # Limit to 15 concurrent requests
        
        # Initialize prompt manager with exact prompts from worker_main.py
        self.prompt_manager = PromptManager()

    def fetch_taxonomy_data(self) -> None:
        """Fetch complete taxonomy hierarchy from GraphQL and build nested structure"""
        query_file = os.path.join(os.path.dirname(__file__), 'graphql', 'GetCompleteTaxonomyHierarchy.graphql')
        with open(query_file, 'r') as f:
            query_text = f.read()
        
        query = gql(query_text)
        variables = {"dcm_id": self.dcm_id}
        result = self.graphql_client.execute(query, variable_values=variables)
        
        print(f"Processing taxonomy data for DCM: {self.dcm_id}")
        
        # Process the taxonomy hierarchy and build nested structure
        for product_item in result['taxonomy_items']:
            if product_item['category'] == 'product_type':
                # Create product node (root)
                product_taxonomy = TaxonomyItem.create_from_graphql("product", product_item)
                product_node = HierarchyNode(product_taxonomy)
                
                # Process product-level modifiers
                self._process_product_modifiers(product_node, product_item)
                
                # Process segments
                for segment_rel in product_item['parent_relationships']:
                    segment_item = segment_rel['related_taxonomy_item']
                    if segment_item['category'] == 'segment_type':
                        self._process_segment(product_node, segment_rel, segment_item)
                
                self.hierarchy_nodes.append(product_node)
        
        # Count all nodes for reporting
        total_nodes = sum(self._count_nodes(node) for node in self.hierarchy_nodes)
        print(f"Built hierarchy with {total_nodes} total nodes")

    def _process_product_modifiers(self, product_node: HierarchyNode, product_item: Dict) -> None:
        """Process product-level modifiers (conditions, limits, exclusions)"""
        for modifier_group in ['product_conditions', 'product_limits', 'product_exclusions']:
            for modifier_rel in product_item.get(modifier_group, []):
                modifier_item = modifier_rel['related_taxonomy_item']
                taxonomy_item = TaxonomyItem.create_from_graphql(modifier_rel['id'], modifier_item)
                modifier_node = HierarchyNode(taxonomy_item)
                product_node.add_child(modifier_node)

    def _process_segment(self, product_node: HierarchyNode, segment_rel: Dict, segment_item: Dict) -> None:
        """Process a segment and its related benefits and modifiers"""
        # Create segment node
        segment_taxonomy = TaxonomyItem.create_from_graphql(segment_rel['id'], segment_item)
        segment_node = HierarchyNode(segment_taxonomy)
        product_node.add_child(segment_node)
        
        # Process segment-level modifiers
        for modifier_group in ['segment_conditions', 'segment_limits', 'segment_exclusions']:
            for modifier_rel in segment_item.get(modifier_group, []):
                modifier_item = modifier_rel['related_taxonomy_item']
                taxonomy_item = TaxonomyItem.create_from_graphql(modifier_rel['id'], modifier_item)
                modifier_node = HierarchyNode(taxonomy_item)
                segment_node.add_child(modifier_node)
        
        # Process benefits for this segment
        for benefit_rel in segment_item.get('parent_relationships', []):
            benefit_item = benefit_rel['related_taxonomy_item']
            if benefit_item['category'] == 'benefit_type':
                self._process_benefit(segment_node, benefit_rel, benefit_item)

    def _process_benefit(self, segment_node: HierarchyNode, benefit_rel: Dict, benefit_item: Dict) -> None:
        """Process a benefit and its modifiers"""
        # Create benefit node
        benefit_taxonomy = TaxonomyItem.create_from_graphql(benefit_rel['id'], benefit_item)
        benefit_node = HierarchyNode(benefit_taxonomy)
        segment_node.add_child(benefit_node)
        
        # Process benefit-level modifiers
        for modifier_group in ['benefit_conditions', 'benefit_limits', 'benefit_exclusions']:
            for modifier_rel in benefit_item.get(modifier_group, []):
                modifier_item = modifier_rel['related_taxonomy_item']
                taxonomy_item = TaxonomyItem.create_from_graphql(modifier_rel['id'], modifier_item)
                modifier_node = HierarchyNode(taxonomy_item)
                benefit_node.add_child(modifier_node)

    def _count_nodes(self, node: HierarchyNode) -> int:
        """Count total nodes in subtree"""
        return 1 + sum(self._count_nodes(child) for child in node.children)

    def _count_modifiers_recursive(self, nodes: List[HierarchyNode]) -> int:
        """Count all modifier nodes in a list of nodes recursively"""
        count = 0
        for node in nodes:
            # Count this node if it's a modifier
            if node.taxonomy_item.category in ['limit_type', 'condition_type', 'exclusion_type']:
                count += 1
            # Count modifiers in children
            count += self._count_modifiers_recursive(node.children)
        return count

    def _create_fresh_llm(self) -> ChatOpenAI:
        """Create a fresh LLM instance with structured output schema"""
        # Generate JSON schema from AnalysisResult model
        analysis_schema = AnalysisResult.model_json_schema()
        
        return ChatOpenAI(
            model=self.openai_model,
            temperature=0,
            model_kwargs={
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "analysis_result",
                        "description": "Structured analysis result for insurance document processing",
                        "schema": analysis_schema,
                        "strict": True
                    }
                }
            }
        )


    async def _analyze_single_node(self, node: HierarchyNode, document_text: str) -> None:
        """Analyze a single node with semaphore control"""
        async with self.semaphore:
            try:
                taxonomy_item = node.taxonomy_item
                llm = self._create_fresh_llm()
                
                # Build hierarchy context from parent nodes
                hierarchy_context = node.get_hierarchy_context()
                
                # Select appropriate prompt based on category using PromptManager
                if taxonomy_item.category == 'segment_type':
                    prompt_text = self.prompt_manager.create_segment_prompt(taxonomy_item)
                elif taxonomy_item.category == 'benefit_type':
                    prompt_text = self.prompt_manager.create_benefit_prompt(taxonomy_item, hierarchy_context)
                elif taxonomy_item.category in ['limit_type', 'condition_type', 'exclusion_type']:
                    prompt_text = self.prompt_manager.create_modifier_prompt(taxonomy_item, hierarchy_context)
                else:
                    # Fallback for any unknown category
                    prompt_text = self.prompt_manager.create_segment_prompt(taxonomy_item)
                
                # Combine prompt with document
                full_prompt = f"{prompt_text}\n\nZu analysierendes AVB-Dokument:\n\n{document_text}"
                
                response = await llm.ainvoke(full_prompt)
                result_data = json.loads(response.content)
                
                # Add the input prompt to the result data
                result_data["input_prompt"] = full_prompt
                
                # Validate and create AnalysisResult
                node.analysis_result = AnalysisResult(**result_data)
                
                print(f"✓ Analyzed: {taxonomy_item.name} -> {node.analysis_result.is_included}")
                
                # Save to debug file immediately if debug is enabled
                if self.debug_enabled and self.product_id:
                    append_debug_result(
                        self.product_id, 
                        "hierarchical_analysis", 
                        taxonomy_item.taxonomy_relationship_id, 
                        node.analysis_result,
                        taxonomy_item.category
                    )
                
            except Exception as e:
                print(f"✗ Error analyzing {node.taxonomy_item.name}: {e}")
                # Set default result on error
                node.analysis_result = AnalysisResult(
                    item_name=node.taxonomy_item.name,
                    is_included=False,
                    section_reference="",
                    full_text_part="",
                    llm_summary="Analysis failed",
                    description="",
                    unit="",
                    value="",
                    input_prompt=f"Error occurred while generating prompt for {node.taxonomy_item.name}"
                )
                
                # Save error result to debug file immediately if debug is enabled
                if self.debug_enabled and self.product_id:
                    append_debug_result(
                        self.product_id, 
                        "hierarchical_analysis", 
                        node.taxonomy_item.taxonomy_relationship_id, 
                        node.analysis_result,
                        node.taxonomy_item.category
                    )

    async def _analyze_hierarchy_recursive(self, nodes: List[HierarchyNode], document_text: str) -> None:
        """Analyze hierarchy recursively, depth-first to complete nested structures"""
        if not nodes:
            return
        
        # Create tasks for all nodes at this level
        tasks = [self._analyze_single_node(node, document_text) for node in nodes]
        
        # Wait for all nodes at this level to complete
        await asyncio.gather(*tasks)
        
        # Now process children of nodes that were found to be included
        child_tasks = []
        skipped_modifiers = 0
        
        for node in nodes:
            if node.analysis_result and node.analysis_result.is_included and node.children:
                print(f"📋 {node.taxonomy_item.name} is included - analyzing {len(node.children)} child modifiers")
                # Process all children of included nodes (benefits/modifiers)
                child_tasks.append(self._analyze_hierarchy_recursive(node.children, document_text))
            elif node.children:
                # Count skipped modifiers for reporting
                skipped_count = self._count_modifiers_recursive(node.children)
                skipped_modifiers += skipped_count
                if skipped_count > 0:
                    print(f"⏭️  {node.taxonomy_item.name} not included - skipping {skipped_count} related modifiers")
        
        if skipped_modifiers > 0:
            print(f"🚫 Total skipped modifiers due to parent exclusion: {skipped_modifiers}")
        
        # Process all child hierarchies in parallel
        if child_tasks:
            await asyncio.gather(*child_tasks)

    def _collect_debug_results_from_nodes(self, nodes: List[HierarchyNode]) -> Dict[str, AnalysisResult]:
        """Collect analysis results from nodes for debug storage, using taxonomy_relationship_id as key"""
        results = {}
        for node in nodes:
            if node.analysis_result:
                results[node.taxonomy_item.taxonomy_relationship_id] = node.analysis_result
            # Recursively collect from children
            child_results = self._collect_debug_results_from_nodes(node.children)
            results.update(child_results)
        return results

    def _apply_debug_results_to_nodes(self, nodes: List[HierarchyNode], debug_results: Dict[str, AnalysisResult]) -> None:
        """Apply loaded debug results back to hierarchy nodes"""
        for node in nodes:
            taxonomy_rel_id = node.taxonomy_item.taxonomy_relationship_id
            if taxonomy_rel_id in debug_results:
                node.analysis_result = debug_results[taxonomy_rel_id]
                print(f"🔄 Restored analysis result for: {node.taxonomy_item.name} -> {node.analysis_result.is_included}")
            # Recursively apply to children
            self._apply_debug_results_to_nodes(node.children, debug_results)

    def _collect_results_hierarchical(self, nodes: List[HierarchyNode]) -> List[Dict]:
        """Collect results in hierarchical format expected by DirectusSeeder"""
        hierarchical_segments = []
        
        print(f"🔍 Collecting hierarchical results from {len(nodes)} nodes")
        
        for node in nodes:
            print(f"🔍 Checking node: {node.taxonomy_item.name} (category: {node.taxonomy_item.category})")
            
            if node.analysis_result:
                print(f"  📊 Has analysis result: {node.analysis_result.item_name} -> {node.analysis_result.is_included}")
            else:
                print(f"  ❌ No analysis result")
                
            if node.analysis_result and node.taxonomy_item.category == 'segment_type' and node.analysis_result.is_included:
                print(f"  ✅ Adding segment: {node.analysis_result.item_name}")
                
                # Build segment entry with nested benefits and modifiers
                segment_data = {
                    "taxonomy_relationship_id": node.taxonomy_item.taxonomy_relationship_id,
                    "item_name": node.analysis_result.item_name,
                    "is_included": node.analysis_result.is_included,
                    "section_reference": node.analysis_result.section_reference,
                    "full_text_part": node.analysis_result.full_text_part,
                    "llm_summary": node.analysis_result.llm_summary,
                    "description": node.analysis_result.description,
                    "unit": node.analysis_result.unit,
                    "value": node.analysis_result.value,
                    "input_prompt": node.analysis_result.input_prompt,
                    "benefits": [],
                    "conditions": [],
                    "limits": [],
                    "exclusions": []
                }
                
                # Process children recursively to find benefits and modifiers
                self._collect_children_hierarchical(node.children, segment_data)
                
                segment_entry = {node.taxonomy_item.taxonomy_relationship_id: segment_data}
                hierarchical_segments.append(segment_entry)
                
            # Recursively check children
            if node.children:
                print(f"  🔍 Checking {len(node.children)} children...")
                child_segments = self._collect_results_hierarchical(node.children)
                hierarchical_segments.extend(child_segments)
        
        print(f"🎯 Total hierarchical segments found: {len(hierarchical_segments)}")
        return hierarchical_segments
    
    def _collect_children_hierarchical(self, nodes: List[HierarchyNode], parent_data: Dict) -> None:
        """Recursively collect children (benefits and modifiers) into parent structure"""
        for node in nodes:
            if node.analysis_result and node.analysis_result.is_included:
                item_data = {
                    "taxonomy_relationship_id": node.taxonomy_item.taxonomy_relationship_id,
                    "item_name": node.analysis_result.item_name,
                    "is_included": node.analysis_result.is_included,
                    "section_reference": node.analysis_result.section_reference,
                    "full_text_part": node.analysis_result.full_text_part,
                    "llm_summary": node.analysis_result.llm_summary,
                    "description": node.analysis_result.description,
                    "unit": node.analysis_result.unit,
                    "value": node.analysis_result.value,
                    "input_prompt": node.analysis_result.input_prompt
                }
                
                if node.taxonomy_item.category == 'benefit_type':
                    # Add benefit with nested modifiers
                    benefit_data = item_data.copy()
                    benefit_data.update({"conditions": [], "limits": [], "exclusions": []})
                    
                    # Process benefit's children (modifiers)
                    self._collect_children_hierarchical(node.children, benefit_data)
                    
                    benefit_entry = {node.taxonomy_item.taxonomy_relationship_id: benefit_data}
                    parent_data["benefits"].append(benefit_entry)
                    
                elif node.taxonomy_item.category == 'condition_type':
                    condition_entry = {node.taxonomy_item.taxonomy_relationship_id: item_data}
                    parent_data["conditions"].append(condition_entry)
                    
                elif node.taxonomy_item.category == 'limit_type':
                    limit_entry = {node.taxonomy_item.taxonomy_relationship_id: item_data}
                    parent_data["limits"].append(limit_entry)
                    
                elif node.taxonomy_item.category == 'exclusion_type':
                    exclusion_entry = {node.taxonomy_item.taxonomy_relationship_id: item_data}
                    parent_data["exclusions"].append(exclusion_entry)
            
            # Process children even if current node is not included (for completeness)
            self._collect_children_hierarchical(node.children, parent_data)

    def _collect_results_recursive(self, nodes: List[HierarchyNode], results: Dict[str, Dict]) -> None:
        """Collect ALL analyzed results from hierarchy recursively - both included and excluded items"""
        for node in nodes:
            if node.analysis_result:  # Collect ALL analyzed items, not just included ones
                category_key = node.taxonomy_item.category.replace('_type', 's')  # segment_type -> segments
                if category_key == 'segments':
                    category_key = 'segments'
                elif category_key == 'benefits':
                    category_key = 'benefits'
                else:
                    # All modifiers go into 'modifiers' category
                    category_key = 'modifiers'
                
                if category_key not in results:
                    results[category_key] = {}
                
                results[category_key][node.taxonomy_item.taxonomy_relationship_id] = {
                    "taxonomy_relationship_id": node.taxonomy_item.taxonomy_relationship_id,
                    "item_name": node.analysis_result.item_name,
                    "is_included": node.analysis_result.is_included,
                    "section_reference": node.analysis_result.section_reference,
                    "full_text_part": node.analysis_result.full_text_part,
                    "llm_summary": node.analysis_result.llm_summary,
                    "description": node.analysis_result.description,
                    "unit": node.analysis_result.unit,
                    "value": node.analysis_result.value,
                    "input_prompt": node.analysis_result.input_prompt
                }
            
            # Always process children regardless of parent inclusion status for complete results
            self._collect_results_recursive(node.children, results)

    async def analyze_document(self, document_text: str, debug: bool = False) -> Dict[str, Any]:
        """Main analysis function - analyze document against nested hierarchy with debug support"""
        print("Starting hierarchical document analysis...")
        print(f"Using semaphore with limit of 15 concurrent requests")
        print(f"📏 Conditional analysis: Modifiers only analyzed for included segments/benefits")
        
        # Set debug flag for incremental saving
        self.debug_enabled = debug
        
        # Try to load existing debug results if debug mode is enabled and product_id is available
        if debug and self.product_id:
            debug_results, is_valid = load_debug_results(self.product_id, "hierarchical_analysis")
            
            if is_valid and debug_results:
                print(f"🔄 Found existing debug file - applying {len(debug_results)} cached results")
                self._apply_debug_results_to_nodes(self.hierarchy_nodes, debug_results)
                print("✅ Analysis skipped - using cached debug results")
            else:
                # Start recursive analysis from root nodes (with incremental debug saving)
                await self._analyze_hierarchy_recursive(self.hierarchy_nodes, document_text)
        else:
            # Regular analysis without debug
            if debug and not self.product_id:
                print("⚠️ Debug mode requested but no product_id provided - running without debug")
            
            # Start recursive analysis from root nodes
            await self._analyze_hierarchy_recursive(self.hierarchy_nodes, document_text)
        
        # Collect all results
        results = {}
        self._collect_results_recursive(self.hierarchy_nodes, results)
        
        # Ensure all categories exist
        for category in ['segments', 'benefits', 'modifiers']:
            if category not in results:
                results[category] = {}
        
        # Count included vs excluded items for better reporting
        segments_included = sum(1 for item in results['segments'].values() if item['is_included'])
        segments_excluded = len(results['segments']) - segments_included
        
        benefits_included = sum(1 for item in results['benefits'].values() if item['is_included'])
        benefits_excluded = len(results['benefits']) - benefits_included
        
        modifiers_included = sum(1 for item in results['modifiers'].values() if item['is_included'])
        modifiers_excluded = len(results['modifiers']) - modifiers_included
        
        print(f"\n🎯 Hierarchical Analysis Complete:")
        print(f"  - {len(results['segments'])} segments analyzed ({segments_included} included, {segments_excluded} excluded)")
        print(f"  - {len(results['benefits'])} benefits analyzed ({benefits_included} included, {benefits_excluded} excluded)")
        print(f"  - {len(results['modifiers'])} modifiers analyzed ({modifiers_included} included, {modifiers_excluded} excluded)")
        print(f"💡 Note: All analyzed items (included and excluded) are exported with reasoning")
        
        return results
    
    def get_hierarchical_results_for_seeding(self) -> Dict[str, Any]:
        """Get results in hierarchical format expected by DirectusSeeder"""
        # If hierarchy_nodes is empty, we need to build the taxonomy first
        if not self.hierarchy_nodes:
            print(f"🔄 Hierarchy is empty, fetching taxonomy data...")
            self.fetch_taxonomy_data()
        
        # If we have a product_id, try to load debug results with category information
        if self.product_id:
            print(f"🔄 Attempting to load debug results for product: {self.product_id}")
            debug_results, categories, is_valid = load_debug_results_with_categories(self.product_id, "hierarchical_analysis")
            
            if is_valid and debug_results:
                print(f"✓ Loaded {len(debug_results)} debug results with category info, applying to hierarchy...")
                # Apply debug results to the hierarchy nodes
                self._apply_debug_results_to_nodes(self.hierarchy_nodes, debug_results)
                print(f"✓ Applied debug results to hierarchy nodes")
                
                # Print summary of categories found
                if categories:
                    category_counts = {}
                    for cat in categories.values():
                        category_counts[cat] = category_counts.get(cat, 0) + 1
                    print(f"📊 Entity categories loaded: {dict(category_counts)}")
            else:
                print(f"❌ No valid debug results found for product: {self.product_id}")
        
        hierarchical_segments = self._collect_results_hierarchical(self.hierarchy_nodes)
        
        return {
            'segments': hierarchical_segments,
            'benefits': [],  # Empty since nested under segments
            'modifiers': []  # Empty since nested under segments  
        }


# Simple test function
async def main():
    """Test the simplified analyzer"""
    analyzer = SimplifiedDocumentAnalyzer(dcm_id="test", product_id="test-product")
    analyzer.fetch_taxonomy_data()
    
    # Test with a simple document
    test_document = "This is a test insurance document..."
    results = await analyzer.analyze_document(test_document, debug=True)
    
    print("\nResults:", json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())