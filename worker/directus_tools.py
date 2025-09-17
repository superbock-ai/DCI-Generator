"""
Directus Data Tools Module for Analysis Results

This module provides functions to insert insurance analysis results into Directus
while maintaining hierarchical relationships between:
- dcm_product (existing insurance product)
- insurance_dcm_segment
- insurance_dcm_benefit
- insurance_dcm_condition, insurance_dcm_limit, insurance_dcm_exclusion

The module preserves the hierarchical structure through proper relationship management.

Usage:
    from directus_tools import DirectusConfig, DirectusClient, DirectusSeeder

"""

import os
import json
import requests
from typing import Dict, List, Any
from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import datetime, timezone
import uuid
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

# Load environment variables
load_dotenv()


@dataclass
class DirectusConfig:
    """Configuration for Directus connection"""
    url: str
    auth_token: str


# SeededData class removed - now using direct Directus queries by product_id


@dataclass
class TaxonomyData:
    """Unified taxonomy data structure containing both analysis data and relationship mappings"""
    segments: List[Dict]
    benefits: Dict[str, List[Dict]]
    details: Dict[str, Dict[str, List[Dict]]]
    mappings: Dict[str, Dict[str, str]]


class UnifiedTaxonomyFetcher:
    """Unified taxonomy fetcher that retrieves data for both analysis and Directus operations"""
    
    def __init__(self, graphql_url: str, auth_token: str):
        self.graphql_url = graphql_url
        self.auth_token = auth_token
        self._cached_data = None
        
    def fetch_taxonomy_data(self, dcm_id: str, force_refresh: bool = False) -> TaxonomyData:
        """
        Fetch complete taxonomy data for both analysis and relationship mapping.
        
        Args:
            dcm_id: The DCM product ID to fetch taxonomy for
            force_refresh: If True, bypass cache and fetch fresh data
            
        Returns:
            TaxonomyData containing segments, benefits, details, and relationship mappings
        """
        # Return cached data if available and not forcing refresh
        if self._cached_data and not force_refresh:
            return self._cached_data
            
        # Set up GraphQL client
        transport = RequestsHTTPTransport(
            url=self.graphql_url,
            headers={'Authorization': f'Bearer {self.auth_token}'}
        )
        client = Client(transport=transport)
        
        # Read the GraphQL query
        query_file = os.path.join(os.path.dirname(__file__), 'graphql', 'GetCompleteTaxonomyHierarchy.graphql')
        with open(query_file, 'r') as f:
            query_text = f.read()
        
        query = gql(query_text)
        variables = {"dcm_id": dcm_id}
        
        # Execute the query
        result = client.execute(query, variable_values=variables)
        
        # Initialize data structures
        segments = []
        benefits = {}
        details = {'limits': {}, 'conditions': {}, 'exclusions': {}}
        mappings = {
            'segments': {},
            'benefits': {},
            'conditions': {},
            'limits': {},
            'exclusions': {}
        }
        
        # Process taxonomy items
        for item in result['taxonomy_items']:
            if item['category'] == 'product_type':
                for parent_rel in item['parent_relationships']:
                    segment_item = parent_rel['related_taxonomy_item']
                    if segment_item['category'] == 'segment_type':
                        segment_name = segment_item['taxonomy_item_name']
                        
                        # Extract segment info for analysis
                        segment_data = {
                            'id': segment_item['id'],
                            'name': segment_name,
                            'description': segment_item['description'],
                            'aliases': segment_item['aliases'],
                            'examples': segment_item['examples'],
                            'llm_instruction': segment_item['llm_instruction']
                        }
                        segments.append(segment_data)
                        
                        # Map segment for Directus relationships
                        mappings['segments'][segment_name] = parent_rel['id']
                        
                        # Process benefits for this segment
                        segment_benefits = []
                        for benefit_rel in segment_item.get('parent_relationships', []):
                            benefit_item = benefit_rel['related_taxonomy_item']
                            if benefit_item['category'] == 'benefit_type':
                                benefit_name = benefit_item['taxonomy_item_name']
                                
                                # Extract benefit info for analysis
                                benefit_data = {
                                    'id': benefit_item['id'],
                                    'name': benefit_name,
                                    'description': benefit_item['description'],
                                    'aliases': benefit_item['aliases'],
                                    'examples': benefit_item['examples'],
                                    'llm_instruction': benefit_item['llm_instruction'],
                                    'segment_name': segment_name,
                                    'unit': benefit_item.get('unit'),
                                    'data_type': benefit_item.get('data_type')
                                }
                                segment_benefits.append(benefit_data)
                                
                                # Map benefit for Directus relationships
                                benefit_key = f"{segment_name}_{benefit_name}"
                                mappings['benefits'][benefit_key] = benefit_rel['id']
                                
                                # Process details (limits, conditions, exclusions)
                                self._process_benefit_details(
                                    benefit_item, benefit_name, segment_name, 
                                    details, mappings
                                )
                        
                        benefits[segment_name] = segment_benefits
                        
                        # Process segment-level details
                        self._process_segment_details(
                            segment_item, segment_name, mappings
                        )
        
        # Cache and return the data
        taxonomy_data = TaxonomyData(
            segments=segments,
            benefits=benefits, 
            details=details,
            mappings=mappings
        )
        self._cached_data = taxonomy_data
        return taxonomy_data
    
    def _process_benefit_details(self, benefit_item: Dict, benefit_name: str, 
                                segment_name: str, details: Dict, mappings: Dict):
        """Process limits, conditions, and exclusions for a benefit"""
        benefit_key = f"{segment_name}_{benefit_name}"
        
        # Process limits
        limits = []
        for limit_rel in benefit_item.get('benefit_limits', []):
            limit_item = limit_rel['related_taxonomy_item']
            limit_name = limit_item['taxonomy_item_name']
            
            limits.append({
                'id': limit_item['id'],
                'name': limit_name,
                'description': limit_item['description'],
                'aliases': limit_item['aliases'],
                'examples': limit_item['examples'],
                'llm_instruction': limit_item.get('llm_instruction', ''),
                'unit': limit_item.get('unit'),
                'data_type': limit_item.get('data_type'),
                'benefit_name': benefit_name,
                'segment_name': segment_name,
                'detail_type': 'limit'
            })
            
            # Map limit for Directus relationships
            limit_key = f"{segment_name}_{benefit_name}_{limit_name}"
            mappings['limits'][limit_key] = limit_rel['id']
        
        details['limits'][benefit_key] = limits
        
        # Process conditions
        conditions = []
        for condition_rel in benefit_item.get('benefit_conditions', []):
            condition_item = condition_rel['related_taxonomy_item']
            condition_name = condition_item['taxonomy_item_name']
            
            conditions.append({
                'id': condition_item['id'],
                'name': condition_name,
                'description': condition_item['description'],
                'aliases': condition_item['aliases'],
                'examples': condition_item['examples'],
                'llm_instruction': condition_item.get('llm_instruction', ''),
                'unit': condition_item.get('unit'),
                'data_type': condition_item.get('data_type'),
                'benefit_name': benefit_name,
                'segment_name': segment_name,
                'detail_type': 'condition'
            })
            
            # Map condition for Directus relationships
            condition_key = f"{segment_name}_{benefit_name}_{condition_name}"
            mappings['conditions'][condition_key] = condition_rel['id']
        
        details['conditions'][benefit_key] = conditions
        
        # Process exclusions
        exclusions = []
        for exclusion_rel in benefit_item.get('benefit_exclusions', []):
            exclusion_item = exclusion_rel['related_taxonomy_item']
            exclusion_name = exclusion_item['taxonomy_item_name']
            
            exclusions.append({
                'id': exclusion_item['id'],
                'name': exclusion_name,
                'description': exclusion_item['description'],
                'aliases': exclusion_item['aliases'],
                'examples': exclusion_item['examples'],
                'llm_instruction': exclusion_item.get('llm_instruction', ''),
                'unit': exclusion_item.get('unit'),
                'data_type': exclusion_item.get('data_type'),
                'benefit_name': benefit_name,
                'segment_name': segment_name,
                'detail_type': 'exclusion'
            })
            
            # Map exclusion for Directus relationships
            exclusion_key = f"{segment_name}_{benefit_name}_{exclusion_name}"
            mappings['exclusions'][exclusion_key] = exclusion_rel['id']
        
        details['exclusions'][benefit_key] = exclusions
    
    def _process_segment_details(self, segment_item: Dict, segment_name: str, mappings: Dict):
        """Process segment-level details (conditions, limits, exclusions)"""
        
        # Process segment-level conditions
        for condition_rel in segment_item.get('segment_conditions', []):
            condition_item = condition_rel['related_taxonomy_item']
            condition_name = condition_item['taxonomy_item_name']
            condition_key = f"{segment_name}_{condition_name}"
            mappings['conditions'][condition_key] = condition_rel['id']
        
        # Process segment-level limits  
        for limit_rel in segment_item.get('segment_limits', []):
            limit_item = limit_rel['related_taxonomy_item']
            limit_name = limit_item['taxonomy_item_name']
            limit_key = f"{segment_name}_{limit_name}"
            mappings['limits'][limit_key] = limit_rel['id']
        
        # Process segment-level exclusions
        for exclusion_rel in segment_item.get('segment_exclusions', []):
            exclusion_item = exclusion_rel['related_taxonomy_item']
            exclusion_name = exclusion_item['taxonomy_item_name']
            exclusion_key = f"{segment_name}_{exclusion_name}"
            mappings['exclusions'][exclusion_key] = exclusion_rel['id']

class DirectusClient:
    """Client for interacting with Directus API"""

    def __init__(self, config: DirectusConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {config.auth_token}',
            'Content-Type': 'application/json'
        })

    def _make_request(self, method: str, endpoint: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make HTTP request to Directus API"""
        url = f"{self.config.url}/items/{endpoint}"

        try:
            if method.upper() == 'GET':
                response = self.session.get(url, params=data)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data)
            elif method.upper() == 'DELETE':
                response = self.session.delete(url)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            raise

    def create_item(self, collection: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create item in Directus collection"""
        result = self._make_request('POST', collection, data)
        return result.get('data', result)

    def get_items(self, collection: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Get items from Directus collection"""
        result = self._make_request('GET', collection, filters if filters is not None else {})
        return result.get('data', [])

    def delete_item(self, collection: str, item_id: str) -> None:
        """Delete item from Directus collection"""
        url = f"{self.config.url}/items/{collection}/{item_id}"
        try:
            response = self.session.delete(url)
            response.raise_for_status()
            # DELETE requests often return empty responses (204 No Content)
            # Don't try to parse JSON from empty responses
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            raise


def sanitize_and_truncate_text(text: str, max_length: int = 300) -> str:
    """
    Sanitize text to ensure it's valid UTF-8, remove problematic characters, and truncate to max length.
    
    Args:
        text: Input text that may contain invalid UTF-8 sequences
        max_length: Maximum length of the text (default: 300)
        
    Returns:
        Sanitized and truncated text safe for database storage
    """
    if not text or not isinstance(text, str):
        return text
    
    # Remove null bytes and other problematic control characters
    sanitized = text.replace('\x00', '')  # Remove null bytes
    sanitized = sanitized.replace('\x01', '')  # Remove start of heading
    sanitized = sanitized.replace('\x02', '')  # Remove start of text
    sanitized = sanitized.replace('\x03', '')  # Remove end of text
    sanitized = sanitized.replace('\x04', '')  # Remove end of transmission
    sanitized = sanitized.replace('\x05', '')  # Remove enquiry
    sanitized = sanitized.replace('\x06', '')  # Remove acknowledge
    sanitized = sanitized.replace('\x07', '')  # Remove bell
    sanitized = sanitized.replace('\x08', '')  # Remove backspace
    # Keep \x09 (tab) and \x0A (newline)
    sanitized = sanitized.replace('\x0B', '')  # Remove vertical tab
    sanitized = sanitized.replace('\x0C', '')  # Remove form feed
    # Keep \x0D (carriage return)
    sanitized = sanitized.replace('\x0E', '')  # Remove shift out
    sanitized = sanitized.replace('\x0F', '')  # Remove shift in
    
    # Remove other control characters (0x10-0x1F except tab, newline, carriage return)
    for i in range(0x10, 0x20):
        if i not in [0x09, 0x0A, 0x0D]:  # Keep tab, newline, carriage return
            sanitized = sanitized.replace(chr(i), '')
    
    # Ensure the string is valid UTF-8 by encoding/decoding
    try:
        sanitized = sanitized.encode('utf-8', errors='ignore').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # If there are still issues, replace problematic characters
        sanitized = sanitized.encode('utf-8', errors='replace').decode('utf-8')
    
    # Truncate to max_length
    if len(sanitized) <= max_length:
        return sanitized
    return sanitized[:max_length] + "..."

class DirectusSeeder:
    """Main seeder class for inserting analysis results into Directus"""

    def __init__(self, client: DirectusClient, dry_run: bool = False):
        self.client = client
        self.dry_run = dry_run
        self.taxonomy_mappings = {}
        self.taxonomy_fetcher = None

    def fetch_taxonomy_mappings(self, dcm_id: str, graphql_url: str, auth_token: str):
        """Fetch taxonomy item mappings from GraphQL endpoint using unified fetcher"""
        print("Fetching taxonomy mappings...")
        
        try:
            # Initialize the unified taxonomy fetcher if not already done
            if not self.taxonomy_fetcher:
                self.taxonomy_fetcher = UnifiedTaxonomyFetcher(graphql_url, auth_token)
            
            # Fetch the unified taxonomy data
            taxonomy_data = self.taxonomy_fetcher.fetch_taxonomy_data(dcm_id)
            
            # Extract just the mappings for the seeder
            self.taxonomy_mappings = taxonomy_data.mappings
            print(f"✓ Loaded taxonomy mappings: {len(self.taxonomy_mappings['segments'])} segments, {len(self.taxonomy_mappings['benefits'])} benefits")
            
        except Exception as e:
            print(f"Error fetching taxonomy mappings: {e}")
            print("Continuing without taxonomy mappings - items may fail to create")
            self.taxonomy_mappings = {'segments': {}, 'benefits': {}, 'conditions': {}, 'limits': {}, 'exclusions': {}}

    def set_taxonomy_data(self, taxonomy_data):
        """Set pre-fetched taxonomy data from unified fetcher to avoid duplicate GraphQL calls"""
        self.taxonomy_mappings = taxonomy_data.mappings
        print(f"✓ Using pre-fetched taxonomy mappings: {len(self.taxonomy_mappings['segments'])} segments, {len(self.taxonomy_mappings['benefits'])} benefits")


    def create_segment(self, segment_key: str, segment_data: Dict[str, Any], product_id: str, fallback_doc_ref: str = None) -> str:
        """Create insurance_dcm_segment entry and return its ID"""

        # Get taxonomy relationship ID for this segment
        segment_name = segment_data.get('item_name', segment_key)
        taxonomy_rel_id = self.taxonomy_mappings.get('segments', {}).get(segment_name)

        if not taxonomy_rel_id:
            print(f"  ⚠ No taxonomy mapping found for segment: {segment_name}")
            # Try to find by key if item_name lookup failed
            taxonomy_rel_id = self.taxonomy_mappings.get('segments', {}).get(segment_key)

        # Use analysis document_reference if available, otherwise fallback to product's document_reference
        doc_ref = segment_data.get('document_reference') or fallback_doc_ref

        data = {
            'status': 'published',
            'segment_name': sanitize_and_truncate_text(segment_name),
            'description': sanitize_and_truncate_text(segment_data.get('description'), 200),
            'dcm_product': product_id,
            'taxonomy_item_relationship': taxonomy_rel_id,
            'document_reference': sanitize_and_truncate_text(doc_ref, 200),
            'section_reference': sanitize_and_truncate_text(segment_data.get('section_reference'), 200),
            'full_text_part': sanitize_and_truncate_text(segment_data.get('full_text_part')),
            'llm_summary': sanitize_and_truncate_text(segment_data.get('llm_summary')),
            'extraction_date': datetime.now(timezone.utc).isoformat(),
            'validated_by_human': False
        }

        if self.dry_run:
            print(f"[DRY RUN] Would create segment: {data['segment_name']}")
            return str(uuid.uuid4())

        result = self.client.create_item('insurance_dcm_segment', data)
        segment_id = result['id']
        print(f"  ✓ Created segment: {data['segment_name']} (ID: {segment_id})")
        return segment_id

    def create_benefit(self, benefit_key: str, benefit_data: Dict[str, Any], segment_id: str, segment_name: str, fallback_doc_ref: str = None) -> str:
        """Create insurance_dcm_benefit entry and return its ID"""

        # Get taxonomy relationship ID for this benefit
        benefit_name = benefit_data.get('item_name', benefit_key)
        benefit_mapping_key = f"{segment_name}_{benefit_name}"
        taxonomy_rel_id = self.taxonomy_mappings.get('benefits', {}).get(benefit_mapping_key)

        if not taxonomy_rel_id:
            # Try alternative key combinations
            alt_key = f"{segment_name}_{benefit_key}"
            taxonomy_rel_id = self.taxonomy_mappings.get('benefits', {}).get(alt_key)

        if not taxonomy_rel_id:
            print(f"    ⚠ No taxonomy mapping found for benefit: {benefit_mapping_key}")

        # Handle value attribute - convert all values to strings
        raw_value = benefit_data.get('value')
        cleaned_value = None
        if raw_value and raw_value != 'N/A':
            # Convert all values to strings (Directus expects string values)
            if isinstance(raw_value, float):
                # Convert float to string, removing unnecessary decimal if it's a whole number
                cleaned_value = str(int(raw_value)) if raw_value.is_integer() else str(raw_value)
            else:
                # It's already a string, just sanitize it
                cleaned_value = sanitize_and_truncate_text(str(raw_value))

        # Use analysis document_reference if available, otherwise fallback to product's document_reference
        doc_ref = benefit_data.get('document_reference') or fallback_doc_ref

        data = {
            'status': 'published',
            'benefit_name': sanitize_and_truncate_text(benefit_name),
            'description': sanitize_and_truncate_text(benefit_data.get('description'), 200),
            'insurance_dcm_segment': segment_id,
            'taxonomy_item_relationship': taxonomy_rel_id,
            'actual_value': cleaned_value,
            'unit': sanitize_and_truncate_text(benefit_data.get('unit')) if benefit_data.get('unit') != 'N/A' else None,
            'document_reference': sanitize_and_truncate_text(doc_ref, 200),
            'section_reference': sanitize_and_truncate_text(benefit_data.get('section_reference'), 200),
            'full_text_part': sanitize_and_truncate_text(benefit_data.get('full_text_part')),
            'llm_summary': sanitize_and_truncate_text(benefit_data.get('llm_summary')),
            'extraction_date': datetime.now(timezone.utc).isoformat(),
            'validated_by_human': False
        }

        if self.dry_run:
            print(f"[DRY RUN] Would create benefit: {data['benefit_name']}")
            return str(uuid.uuid4())

        result = self.client.create_item('insurance_dcm_benefit', data)
        benefit_id = result['id']
        print(f"    ✓ Created benefit: {data['benefit_name']} (ID: {benefit_id})")
        return benefit_id

    def create_detail_item(self, detail_type: str, detail_key: str, detail_data: Dict[str, Any],
                          product_id: str = None, segment_id: str = None, benefit_id: str = None,
                          segment_name: str = None, benefit_name: str = None, fallback_doc_ref: str = None) -> str:
        """Create condition, limit, or exclusion entry"""
        collection_map = {
            'conditions': 'insurance_dcm_condition',
            'limits': 'insurance_dcm_limit',
            'exclusions': 'insurance_dcm_exclusion'
        }

        collection = collection_map[detail_type]

        # Get taxonomy relationship ID for this detail item
        detail_name = detail_data.get('item_name', detail_key)
        taxonomy_rel_id = None

        if benefit_name and segment_name:
            # Benefit-level detail
            detail_mapping_key = f"{segment_name}_{benefit_name}_{detail_name}"
            taxonomy_rel_id = self.taxonomy_mappings.get(detail_type, {}).get(detail_mapping_key)
        elif segment_name:
            # Segment-level detail
            detail_mapping_key = f"{segment_name}_{detail_name}"
            taxonomy_rel_id = self.taxonomy_mappings.get(detail_type, {}).get(detail_mapping_key)

        # Use analysis document_reference if available, otherwise fallback to product's document_reference
        doc_ref = detail_data.get('document_reference') or fallback_doc_ref

        # Base data structure with UTF-8 sanitization
        data = {
            'status': 'published',
            'taxonomy_item_relationship': taxonomy_rel_id,
            'document_reference': sanitize_and_truncate_text(doc_ref, 200),
            'section_reference': sanitize_and_truncate_text(detail_data.get('section_reference'), 200),
            'full_text_part': sanitize_and_truncate_text(detail_data.get('full_text_part')),
            'llm_summary': sanitize_and_truncate_text(detail_data.get('llm_summary')),
            'extraction_date': datetime.now(timezone.utc).isoformat(),
            'validated_by_human': False
        }

        # Add specific fields and relationships based on detail type
        if detail_type == 'conditions':
            data['condition_name'] = sanitize_and_truncate_text(detail_data.get('item_name', detail_key))
            data['description'] = sanitize_and_truncate_text(detail_data.get('description'))
            if product_id:
                data['dcm_product'] = product_id
            if segment_id:
                data['insurance_dcm_segment'] = segment_id
            if benefit_id:
                data['insurance_dcm_benefit'] = benefit_id

        elif detail_type == 'limits':
            data['limit_name'] = sanitize_and_truncate_text(detail_data.get('item_name', detail_key))
            data['description'] = sanitize_and_truncate_text(detail_data.get('description'))
            
            # Handle value attribute - if float use directly, if string clean it
            raw_value = detail_data.get('value')
            if raw_value is not None and raw_value not in ['N/A', '', ',']:
                if isinstance(raw_value, float):
                    # Use float value directly
                    data['limit_value'] = raw_value
                else:
                    # It's a string, perform cleaning as before
                    try:
                        clean_value = str(raw_value).replace("'", "").replace(",", "").strip()
                        # Skip empty strings after cleaning
                        if not clean_value:
                            data['limit_value'] = None
                        else:
                            # Try to convert to float first, then int if it's a whole number
                            numeric_value = float(clean_value)
                            if numeric_value.is_integer():
                                data['limit_value'] = int(numeric_value)
                            else:
                                data['limit_value'] = numeric_value
                    except ValueError:
                        # If conversion fails, handle special text values
                        text_value = sanitize_and_truncate_text(str(raw_value).strip())
                        if text_value and text_value not in ['N/A', ',']:
                            # For text values like "Unbegrenzt" (unlimited), store as NULL since it's a numeric field
                            if text_value.lower() in ['unbegrenzt', 'unlimited', 'illimité', 'unlimited coverage']:
                                data['limit_value'] = None  # or use 999999999 for unlimited
                            else:
                                # Store other meaningful text values as NULL for numeric field
                                data['limit_value'] = None
                                print(f"      ⚠ Non-numeric limit value '{text_value}' stored as NULL")
                        else:
                            data['limit_value'] = None
            else:
                data['limit_value'] = None
                
            data['limit_unit'] = sanitize_and_truncate_text(detail_data.get('unit'))
            if product_id:
                data['dcm_product'] = product_id
            if segment_id:
                data['insurance_dcm_segment'] = segment_id
            if benefit_id:
                data['insurance_dcm_benefit'] = benefit_id

        elif detail_type == 'exclusions':
            data['exclusion_name'] = sanitize_and_truncate_text(detail_data.get('item_name', detail_key))
            data['description'] = sanitize_and_truncate_text(detail_data.get('description'))
            if product_id:
                data['dcm_product'] = product_id
            if segment_id:
                data['insurance_dcm_segment'] = segment_id
            if benefit_id:
                data['insurance_dcm_benefit'] = benefit_id

        if self.dry_run:
            level = "product" if product_id and not segment_id else "segment" if segment_id and not benefit_id else "benefit"
            print(f"[DRY RUN] Would create {detail_type[:-1]}: {data.get(f'{detail_type[:-1]}_name')} at {level} level")
            return str(uuid.uuid4())

        result = self.client.create_item(collection, data)
        detail_id = result['id']

        level = "product" if product_id and not segment_id else "segment" if segment_id and not benefit_id else "benefit"
        print(f"      ✓ Created {detail_type[:-1]}: {data.get(f'{detail_type[:-1]}_name')} at {level} level (ID: {detail_id})")
        return detail_id

    def process_details(self, details_data: List[Dict], detail_type: str,
                       product_id: str = None, segment_id: str = None, benefit_id: str = None,
                       segment_name: str = None, benefit_name: str = None, fallback_doc_ref: str = None):
        """Process conditions, limits, or exclusions"""
        for detail_item in details_data:
            for detail_key, detail_data in detail_item.items():
                if detail_data.get('is_included'):
                    self.create_detail_item(detail_type, detail_key, detail_data,
                                          product_id, segment_id, benefit_id,
                                          segment_name, benefit_name, fallback_doc_ref)

    def seed_analysis_results(self, analysis_results: Dict[str, Any], product_id: str, graphql_url: str, auth_token: str, taxonomy_data=None):
        """Main method to seed all analysis results
        
        Args:
            analysis_results: The analysis results to seed
            product_id: ID of the existing dcm_product 
            graphql_url: GraphQL endpoint URL
            auth_token: Authentication token
            taxonomy_data: Optional pre-fetched taxonomy data to avoid duplicate GraphQL calls
        """
        print(f"Starting to seed {product_id} analysis results {'(DRY RUN)' if self.dry_run else ''}")
        print("=" * 60)

        # Initialize fallback document reference
        fallback_doc_ref = None

        # Get DCM ID from existing product and fetch/set taxonomy mappings
        if not self.dry_run:
            print(f"Using existing dcm_product: {product_id}")
            # Get the DCM ID from the existing product
            try:
                product_items = self.client.get_items('dcm_product', {'filter[id][_eq]': product_id})
                if not product_items:
                    print(f"Error: Product with ID {product_id} not found")
                    return
                dcm_id = product_items[0].get('domain_context_model')
                if not dcm_id:
                    print(f"Error: Product {product_id} has no domain_context_model")
                    return
                print(f"✓ Found product with DCM ID: {dcm_id}")
                
                # Extract document reference from product for fallback
                fallback_doc_ref = product_items[0].get('document_reference')
                if fallback_doc_ref:
                    print(f"✓ Using product document_reference as fallback: {fallback_doc_ref}")
                else:
                    print("⚠ No document_reference found in product")
                
                # Use pre-fetched taxonomy data if provided, otherwise fetch fresh data
                if taxonomy_data:
                    print("Using pre-fetched taxonomy data from analysis phase...")
                    self.set_taxonomy_data(taxonomy_data)
                else:
                    print("Fetching fresh taxonomy data...")
                    self.fetch_taxonomy_mappings(dcm_id, graphql_url, auth_token)
                    
            except Exception as e:
                print(f"Error fetching product info: {e}")
                return
        else:
            print(f"[DRY RUN] Would use existing dcm_product: {product_id}")
            fallback_doc_ref = "document-reference-from-product"  # Mock for dry run
            if taxonomy_data:
                print("[DRY RUN] Would use pre-fetched taxonomy data")
                self.set_taxonomy_data(taxonomy_data) 
            else:
                print("[DRY RUN] Skipping taxonomy mapping fetch")

        # Get the product data
        product_data = analysis_results.get(product_id, {})
        segments_data = product_data.get('segments', [])

        if not segments_data:
            print("No segments found in analysis results")
            return

        # No tracking needed - we can query Directus directly by product_id

        # Process each segment
        for segment_item in segments_data:
            for segment_key, segment_data in segment_item.items():
                if not segment_data.get('is_included'):
                    continue

                print(f"\nProcessing segment: {segment_key}")
                segment_id = self.create_segment(segment_key, segment_data, product_id, fallback_doc_ref)
                segment_name = segment_data.get('item_name', segment_key)

                # Process segment-level details
                if 'conditions' in segment_data:
                    self.process_details(segment_data['conditions'], 'conditions',
                                       product_id=product_id, segment_id=segment_id,
                                       segment_name=segment_name, fallback_doc_ref=fallback_doc_ref)
                if 'limits' in segment_data:
                    self.process_details(segment_data['limits'], 'limits',
                                       product_id=product_id, segment_id=segment_id,
                                       segment_name=segment_name, fallback_doc_ref=fallback_doc_ref)
                if 'exclusions' in segment_data:
                    self.process_details(segment_data['exclusions'], 'exclusions',
                                       product_id=product_id, segment_id=segment_id,
                                       segment_name=segment_name, fallback_doc_ref=fallback_doc_ref)

                # Process benefits within this segment
                benefits_data = segment_data.get('benefits', [])
                for benefit_item in benefits_data:
                    for benefit_key, benefit_data in benefit_item.items():
                        if not benefit_data.get('is_included'):
                            continue

                        benefit_id = self.create_benefit(benefit_key, benefit_data, segment_id, segment_name, fallback_doc_ref)
                        benefit_name = benefit_data.get('item_name', benefit_key)

                        # Process benefit-level details
                        if 'conditions' in benefit_data:
                            self.process_details(benefit_data['conditions'], 'conditions',
                                               product_id=product_id, segment_id=segment_id, benefit_id=benefit_id,
                                               segment_name=segment_name, benefit_name=benefit_name, fallback_doc_ref=fallback_doc_ref)
                        if 'limits' in benefit_data:
                            self.process_details(benefit_data['limits'], 'limits',
                                               product_id=product_id, segment_id=segment_id, benefit_id=benefit_id,
                                               segment_name=segment_name, benefit_name=benefit_name, fallback_doc_ref=fallback_doc_ref)
                        if 'exclusions' in benefit_data:
                            self.process_details(benefit_data['exclusions'], 'exclusions',
                                               product_id=product_id, segment_id=segment_id, benefit_id=benefit_id,
                                               segment_name=segment_name, benefit_name=benefit_name, fallback_doc_ref=fallback_doc_ref)

        print(f"\n✓ Seeding completed {'(DRY RUN)' if self.dry_run else ''}!")

    # print_summary method removed - no longer tracking seeded data in memory

    def cleanup_by_product_id(self, product_id: str):
        """Delete all data associated with a dcm_product"""
        print(f"Cleaning up all data for product: {product_id}")
        print("=" * 60)

        # Verify the product exists
        try:
            product_items = self.client.get_items('dcm_product', {'filter[id][_eq]': product_id})
            if not product_items:
                print(f"Error: Product with ID {product_id} not found")
                return False
            print(f"✓ Found product: {product_items[0].get('product_name', product_id)}")
        except Exception as e:
            print(f"Error fetching product: {e}")
            return False

        # Query and delete in reverse dependency order to respect foreign key constraints
        deletion_stats = {
            'exclusions': 0,
            'limits': 0,
            'conditions': 0,
            'benefits': 0,
            'segments': 0,
            'product': 0
        }

        # 1. Delete exclusions (no dependencies)
        print("\n1. Deleting exclusions...")
        try:
            exclusions = self.client.get_items('insurance_dcm_exclusion', {'filter[dcm_product][_eq]': product_id})
            for exclusion in exclusions:
                try:
                    self.client.delete_item('insurance_dcm_exclusion', exclusion['id'])
                    deletion_stats['exclusions'] += 1
                    print(f"  ✓ Deleted exclusion: {exclusion.get('exclusion_name', exclusion['id'])}")
                except Exception as e:
                    print(f"  ✗ Failed to delete exclusion {exclusion['id']}: {e}")
        except Exception as e:
            print(f"Error fetching exclusions: {e}")

        # 2. Delete limits (no dependencies)
        print("\n2. Deleting limits...")
        try:
            limits = self.client.get_items('insurance_dcm_limit', {'filter[dcm_product][_eq]': product_id})
            for limit in limits:
                try:
                    self.client.delete_item('insurance_dcm_limit', limit['id'])
                    deletion_stats['limits'] += 1
                    print(f"  ✓ Deleted limit: {limit.get('limit_name', limit['id'])}")
                except Exception as e:
                    print(f"  ✗ Failed to delete limit {limit['id']}: {e}")
        except Exception as e:
            print(f"Error fetching limits: {e}")

        # 3. Delete conditions (no dependencies)
        print("\n3. Deleting conditions...")
        try:
            conditions = self.client.get_items('insurance_dcm_condition', {'filter[dcm_product][_eq]': product_id})
            for condition in conditions:
                try:
                    self.client.delete_item('insurance_dcm_condition', condition['id'])
                    deletion_stats['conditions'] += 1
                    print(f"  ✓ Deleted condition: {condition.get('condition_name', condition['id'])}")
                except Exception as e:
                    print(f"  ✗ Failed to delete condition {condition['id']}: {e}")
        except Exception as e:
            print(f"Error fetching conditions: {e}")

        # 4. Delete benefits (depends on limits/conditions/exclusions)
        print("\n4. Deleting benefits...")
        try:
            # Get benefits via segments
            segments = self.client.get_items('insurance_dcm_segment', {'filter[dcm_product][_eq]': product_id})
            for segment in segments:
                segment_benefits = self.client.get_items('insurance_dcm_benefit', {'filter[insurance_dcm_segment][_eq]': segment['id']})
                for benefit in segment_benefits:
                    try:
                        self.client.delete_item('insurance_dcm_benefit', benefit['id'])
                        deletion_stats['benefits'] += 1
                        print(f"  ✓ Deleted benefit: {benefit.get('benefit_name', benefit['id'])}")
                    except Exception as e:
                        print(f"  ✗ Failed to delete benefit {benefit['id']}: {e}")
        except Exception as e:
            print(f"Error fetching/deleting benefits: {e}")

        # 5. Delete segments (depends on benefits)
        print("\n5. Deleting segments...")
        try:
            segments = self.client.get_items('insurance_dcm_segment', {'filter[dcm_product][_eq]': product_id})
            for segment in segments:
                try:
                    self.client.delete_item('insurance_dcm_segment', segment['id'])
                    deletion_stats['segments'] += 1
                    print(f"  ✓ Deleted segment: {segment.get('segment_name', segment['id'])}")
                except Exception as e:
                    print(f"  ✗ Failed to delete segment {segment['id']}: {e}")
        except Exception as e:
            print(f"Error fetching/deleting segments: {e}")

        # 6. Delete the product (depends on segments)
        print(f"\n6. Deleting product {product_id}...")
        try:
            self.client.delete_item('dcm_product', product_id)
            deletion_stats['product'] = 1
            print(f"  ✓ Deleted product: {product_id}")
        except Exception as e:
            print(f"  ✗ Failed to delete product {product_id}: {e}")

        # Summary
        print(f"\n✓ Cleanup completed!")
        print("Summary:")
        print(f"- Exclusions: {deletion_stats['exclusions']} deleted")
        print(f"- Limits: {deletion_stats['limits']} deleted")
        print(f"- Conditions: {deletion_stats['conditions']} deleted")
        print(f"- Benefits: {deletion_stats['benefits']} deleted")
        print(f"- Segments: {deletion_stats['segments']} deleted")
        print(f"- Product: {deletion_stats['product']} deleted")

        total_deleted = sum(deletion_stats.values())
        print(f"- Total items: {total_deleted} deleted")

        return True

# Module can be imported and used via seed_to_directus() and cleanup_seeded_data() functions