"""
Clean Document Analyzer with proper separation of concerns
"""

import asyncio
from typing import Dict, List, Any, Optional
from models import AnalysisResult, HierarchyNode
from utils import AnalyzerConfig, AnalysisLogger
from services import LLMService, GraphQLService
from prompt_manager import PromptManager


class HierarchyProcessor:
    """Handles hierarchy traversal and analysis logic"""
    
    def __init__(self, llm_service: LLMService, prompt_manager: PromptManager, logger: AnalysisLogger, debug_manager=None, product_id=None):
        self.llm_service = llm_service
        self.prompt_manager = prompt_manager
        self.logger = logger
        self.debug_manager = debug_manager
        self.product_id = product_id
        self.debug_enabled = False
    
    async def analyze_node_with_context(self, node: HierarchyNode, document_text: str, semaphore: asyncio.Semaphore) -> None:
        """Analyze a single node with hierarchy context"""
        async with semaphore:
            try:
                taxonomy_item = node.taxonomy_item
                
                # Build hierarchy context from parent nodes
                hierarchy_context = node.get_hierarchy_context()
                
                # Select appropriate prompt based on category
                prompt_text = self._get_prompt_for_category(taxonomy_item.category, taxonomy_item, hierarchy_context)
                
                # Combine prompt with document
                full_prompt = f"{prompt_text}\n\nZu analysierendes AVB-Dokument:\n\n{document_text}"
                
                # Use clean LLMService with native structured output
                node.analysis_result = await self.llm_service.analyze_with_structured_output(
                    full_prompt, 
                    taxonomy_item.name
                )
                
                # CONTINUOUS DEBUG WRITING - Save immediately after each analysis
                if self.debug_enabled and self.debug_manager and self.product_id:
                    self.debug_manager.append_debug_result(
                        self.product_id, 
                        "hierarchical_analysis", 
                        taxonomy_item.taxonomy_relationship_id, 
                        node.analysis_result,
                        taxonomy_item.category
                    )
                
            except Exception as e:
                self.logger.analysis_error("node_analysis", node.taxonomy_item.name, str(e))
                # Set default result on error
                node.analysis_result = AnalysisResult(
                    item_name=node.taxonomy_item.name,
                    is_included=False,
                    section_reference="",
                    full_text_part="",
                    llm_summary="Analysis failed",
                    description="",
                    unit="",
                    value=None,
                    unlimited=False,
                    input_prompt=f"Error occurred while generating prompt for {node.taxonomy_item.name}"
                )
                
                # CONTINUOUS DEBUG WRITING - Save error result immediately
                if self.debug_enabled and self.debug_manager and self.product_id:
                    self.debug_manager.append_debug_result(
                        self.product_id, 
                        "hierarchical_analysis", 
                        node.taxonomy_item.taxonomy_relationship_id, 
                        node.analysis_result,
                        node.taxonomy_item.category
                    )
    
    def _get_prompt_for_category(self, category: str, taxonomy_item, hierarchy_context: str) -> str:
        """Get appropriate prompt based on taxonomy category"""
        if category == 'segment_type':
            return self.prompt_manager.create_segment_prompt(taxonomy_item)
        elif category == 'benefit_type':
            return self.prompt_manager.create_benefit_prompt(taxonomy_item, hierarchy_context)
        elif category in ['limit_type', 'condition_type', 'exclusion_type']:
            return self.prompt_manager.create_modifier_prompt(taxonomy_item, hierarchy_context)
        else:
            # Fallback for unknown category
            return self.prompt_manager.create_segment_prompt(taxonomy_item)
    
    async def analyze_hierarchy_recursive(self, nodes: List[HierarchyNode], document_text: str, semaphore: asyncio.Semaphore) -> None:
        """Analyze hierarchy recursively with conditional processing"""
        if not nodes:
            return
        
        # Create tasks for all nodes at this level
        tasks = [self.analyze_node_with_context(node, document_text, semaphore) for node in nodes]
        
        # Wait for all nodes at this level to complete
        await asyncio.gather(*tasks)
        
        # Now process children of nodes that were found to be included
        child_tasks = []
        skipped_modifiers = 0
        
        for node in nodes:
            if node.analysis_result and node.analysis_result.is_included and node.children:
                self.logger.debug_operation("child_analysis", f"{node.taxonomy_item.name} is included - analyzing {len(node.children)} child modifiers")
                child_tasks.append(self.analyze_hierarchy_recursive(node.children, document_text, semaphore))
            elif node.children:
                # Count skipped modifiers for reporting
                skipped_count = self._count_modifiers_recursive(node.children)
                skipped_modifiers += skipped_count
                if skipped_count > 0:
                    self.logger.debug_operation("skip_analysis", f"{node.taxonomy_item.name} not included - skipping {skipped_count} related modifiers")
        
        if skipped_modifiers > 0:
            self.logger.debug_operation("skip_summary", f"Total skipped modifiers due to parent exclusion: {skipped_modifiers}")
        
        # Process all child hierarchies in parallel
        if child_tasks:
            await asyncio.gather(*child_tasks)
    
    def _count_modifiers_recursive(self, nodes: List[HierarchyNode]) -> int:
        """Count all modifier nodes in a list of nodes recursively"""
        count = 0
        for node in nodes:
            if node.taxonomy_item.category in ['limit_type', 'condition_type', 'exclusion_type']:
                count += 1
            count += self._count_modifiers_recursive(node.children)
        return count


class ResultsCollector:
    """Handles collection and formatting of analysis results"""
    
    def __init__(self, logger: AnalysisLogger):
        self.logger = logger
    
    def collect_flat_results(self, nodes: List[HierarchyNode]) -> Dict[str, Dict]:
        """Collect ALL analyzed results from hierarchy recursively"""
        results = {}
        self._collect_results_recursive(nodes, results)
        
        # Ensure all categories exist
        for category in ['segments', 'benefits', 'modifiers']:
            if category not in results:
                results[category] = {}
        
        return results
    
    def collect_hierarchical_results(self, nodes: List[HierarchyNode]) -> List[Dict]:
        """Collect results in hierarchical format for seeding"""
        self.logger.debug_operation("collect_results", f"Collecting hierarchical results from {len(nodes)} nodes")
        hierarchical_segments = []
        
        for node in nodes:
            self.logger.debug_operation("check_node", f"Checking node: {node.taxonomy_item.name} (category: {node.taxonomy_item.category})")
            
            if node.analysis_result:
                self.logger.debug_operation("node_has_result", f"Has analysis result: {node.analysis_result.item_name} -> {node.analysis_result.is_included}")
            else:
                self.logger.debug_operation("node_no_result", "No analysis result")
                
            if node.analysis_result and node.taxonomy_item.category == 'segment_type' and node.analysis_result.is_included:
                self.logger.debug_operation("add_segment", f"Adding segment: {node.analysis_result.item_name}")
                
                # Build segment entry with nested benefits and modifiers
                segment_data = self._build_segment_data(node)
                segment_entry = {node.taxonomy_item.taxonomy_relationship_id: segment_data}
                hierarchical_segments.append(segment_entry)
                
            # Recursively check children
            if node.children:
                self.logger.debug_operation("check_children", f"Checking {len(node.children)} children...")
                child_segments = self.collect_hierarchical_results(node.children)
                hierarchical_segments.extend(child_segments)
        
        self.logger.debug_operation("segments_found", f"Total hierarchical segments found: {len(hierarchical_segments)}")
        return hierarchical_segments
    
    def _build_segment_data(self, node: HierarchyNode) -> Dict:
        """Build complete segment data with nested structure"""
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
        return segment_data
    
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
        """Collect ALL analyzed results from hierarchy recursively"""
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


class DocumentAnalyzer:
    """
    Clean, streamlined document analyzer with proper separation of concerns
    
    This class focuses solely on orchestrating the analysis process,
    delegating specific responsibilities to focused helper classes.
    """
    
    def __init__(self, dcm_id: str, product_id: Optional[str] = None, config: Optional[AnalyzerConfig] = None):
        """Initialize with dependency injection"""
        self.dcm_id = dcm_id
        self.product_id = product_id
        
        # Use provided config or load from environment
        self.config = config or AnalyzerConfig.from_environment()
        
        # Initialize services
        self.llm_service = LLMService(self.config)
        self.graphql_service = GraphQLService(self.config) 
        
        # Initialize processors
        from utils import get_analysis_logger, DebugManager
        self.logger = get_analysis_logger()
        self.prompt_manager = PromptManager()
        self.debug_manager = DebugManager(self.config.debug_base_dir) if product_id else None
        
        # Initialize specialized processors with debug support
        self.hierarchy_processor = HierarchyProcessor(
            self.llm_service, 
            self.prompt_manager, 
            self.logger,
            debug_manager=self.debug_manager,
            product_id=self.product_id
        )
        self.results_collector = ResultsCollector(self.logger)
        
        # Analysis state
        self.hierarchy_nodes: List[HierarchyNode] = []
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        
        self.logger.debug_operation("analyzer_init", f"Initialized clean DocumentAnalyzer for DCM {dcm_id}")
    
    def load_taxonomy(self) -> None:
        """Load taxonomy data from GraphQL service"""
        self.hierarchy_nodes = self.graphql_service.fetch_taxonomy_data(self.dcm_id)
    
    async def analyze_document(self, document_text: str, enable_debug: bool = False) -> Dict[str, Any]:
        """
        Main analysis method - clean and focused
        """
        # Load taxonomy if not already loaded
        if not self.hierarchy_nodes:
            self.load_taxonomy()
        
        self.logger.analysis_start("hierarchical_analysis", len(self.hierarchy_nodes))
        
        # Handle debug loading
        if enable_debug and self.debug_manager and self.product_id:
            debug_results, is_valid = self.debug_manager.load_debug_results(self.product_id, "hierarchical_analysis")
            if is_valid and debug_results:
                self.logger.cache_operation("debug_cache_hit", len(debug_results), 0)
                self._apply_debug_results(debug_results)
                self.logger.debug_operation("analysis_cached", "Analysis skipped - using cached debug results")
            else:
                await self._perform_analysis(document_text, enable_debug)
        else:
            await self._perform_analysis(document_text, enable_debug)
        
        # Collect and return results
        results = self.results_collector.collect_flat_results(self.hierarchy_nodes)
        self._log_completion_summary(results)
        
        return results
    
    def get_hierarchical_results_for_seeding(self) -> Dict[str, Any]:
        """Get results in hierarchical format for DirectusSeeder"""
        # Load taxonomy if not already loaded
        if not self.hierarchy_nodes:
            self.load_taxonomy()
        
        # Load debug results if available
        if self.debug_manager and self.product_id:
            debug_results, categories, is_valid = self.debug_manager.load_debug_results_with_categories(
                self.product_id, "hierarchical_analysis"
            )
            if is_valid and debug_results:
                self._apply_debug_results(debug_results)
        
        hierarchical_segments = self.results_collector.collect_hierarchical_results(self.hierarchy_nodes)
        
        return {
            'segments': hierarchical_segments,
            'benefits': [],  # Empty since nested under segments
            'modifiers': []  # Empty since nested under segments  
        }
    
    async def _perform_analysis(self, document_text: str, enable_debug: bool) -> None:
        """Perform the actual analysis using hierarchy processor"""
        # Enable debug mode in hierarchy processor
        self.hierarchy_processor.debug_enabled = enable_debug
        
        await self.hierarchy_processor.analyze_hierarchy_recursive(
            self.hierarchy_nodes, 
            document_text, 
            self.semaphore
        )
        
        # Note: Individual debug results are now saved immediately during analysis
        # This final save is kept for any potential missed results
        if enable_debug and self.debug_manager and self.product_id:
            for node in self._get_all_nodes_with_results():
                # Only save if not already saved during analysis
                if node.analysis_result:
                    self.debug_manager.append_debug_result(
                        self.product_id,
                        "hierarchical_analysis", 
                        node.taxonomy_item.taxonomy_relationship_id,
                        node.analysis_result,
                        node.taxonomy_item.category
                    )
    
    def _apply_debug_results(self, debug_results: Dict[str, AnalysisResult]) -> None:
        """Apply loaded debug results to hierarchy nodes"""
        self._apply_debug_results_to_nodes(self.hierarchy_nodes, debug_results)
    
    def _apply_debug_results_to_nodes(self, nodes: List[HierarchyNode], debug_results: Dict[str, AnalysisResult]) -> None:
        """Recursively apply debug results to nodes"""
        for node in nodes:
            taxonomy_rel_id = node.taxonomy_item.taxonomy_relationship_id
            if taxonomy_rel_id in debug_results:
                node.analysis_result = debug_results[taxonomy_rel_id]
                self.logger.debug_operation("restore_result", f"Restored analysis result for: {node.taxonomy_item.name} -> {node.analysis_result.is_included}")
            # Recursively apply to children
            self._apply_debug_results_to_nodes(node.children, debug_results)
    
    def _get_all_nodes_with_results(self) -> List[HierarchyNode]:
        """Get all nodes that have analysis results"""
        nodes_with_results = []
        self._collect_nodes_with_results(self.hierarchy_nodes, nodes_with_results)
        return nodes_with_results
    
    def _collect_nodes_with_results(self, nodes: List[HierarchyNode], result_list: List[HierarchyNode]) -> None:
        """Recursively collect nodes that have analysis results"""
        for node in nodes:
            if node.analysis_result:
                result_list.append(node)
            self._collect_nodes_with_results(node.children, result_list)
    
    def _log_completion_summary(self, results: Dict[str, Any]) -> None:
        """Log analysis completion summary"""
        segments_included = sum(1 for item in results['segments'].values() if item['is_included'])
        benefits_included = sum(1 for item in results['benefits'].values() if item['is_included'])
        modifiers_included = sum(1 for item in results['modifiers'].values() if item['is_included'])
        
        self.logger.analysis_complete("segment_analysis", segments_included, len(results['segments']))
        self.logger.analysis_complete("benefit_analysis", benefits_included, len(results['benefits']))
        self.logger.analysis_complete("modifier_analysis", modifiers_included, len(results['modifiers']))
        self.logger.debug_operation("export_note", "All analyzed items (included and excluded) are exported with reasoning")