"""
GraphQL Service for the DCI Generator
"""

import os
from typing import Any, Dict, List
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from models import TaxonomyItem, HierarchyNode
from utils import AnalyzerConfig, get_analysis_logger


class GraphQLService:
    """Service for managing GraphQL operations and taxonomy data"""
    
    def __init__(self, config: AnalyzerConfig):
        """Initialize the GraphQL service with configuration"""
        self.config = config
        self.logger = get_analysis_logger()
        
        # Initialize GraphQL client
        transport = RequestsHTTPTransport(
            url=self.config.graphql_url,
            headers={"Authorization": f"Bearer {self.config.directus_auth_token}"},
            use_json=True
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
        
        self.logger.debug_operation("graphql_service_init", f"Initialized GraphQL service for: {self.config.graphql_url}")
    
    def fetch_taxonomy_data(self, dcm_id: str) -> List[HierarchyNode]:
        """
        Fetch complete taxonomy hierarchy from GraphQL and build nested structure
        
        Args:
            dcm_id: The DCM identifier to fetch taxonomy for
            
        Returns:
            List of root HierarchyNode objects representing the complete taxonomy
        """
        self.logger.debug_operation("taxonomy_fetch", f"Fetching taxonomy data for DCM: {dcm_id}")
        
        # Load GraphQL query from file
        query_file = os.path.join(os.path.dirname(__file__), '..', 'graphql', 'GetCompleteTaxonomyHierarchy.graphql')
        with open(query_file, 'r') as f:
            query_text = f.read()
        
        query = gql(query_text)
        variables = {"dcm_id": dcm_id}
        result = self.client.execute(query, variable_values=variables)
        
        # Process the taxonomy hierarchy and build nested structure
        hierarchy_nodes = []
        
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
                
                hierarchy_nodes.append(product_node)
        
        # Count all nodes for reporting
        total_nodes = sum(self._count_nodes(node) for node in hierarchy_nodes)
        self.logger.debug_operation("taxonomy_build", f"Built hierarchy with {total_nodes} total nodes")
        
        return hierarchy_nodes
    
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
    
    def validate_connection(self) -> bool:
        """
        Validate the GraphQL connection
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            # Simple introspection query to test connection
            test_query = gql("query { __typename }")
            self.client.execute(test_query)
            self.logger.debug_operation("graphql_validation", "Connection validated successfully")
            return True
        except Exception as e:
            self.logger.analysis_error("graphql_validation", "connection", str(e))
            return False