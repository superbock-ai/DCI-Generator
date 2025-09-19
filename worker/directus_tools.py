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
class TaxonomyRelationship:
    """Represents a specific taxonomy relationship with unique UUID"""
    relationship_id: str  # The unique relationship UUID from parent_relationships[].id
    taxonomy_item_id: str  # The taxonomy item this relationship points to
    taxonomy_item_name: str  # Name of the taxonomy item (NOT unique across relationships!)
    category: str  # category of the related taxonomy item
    parent_category: str  # category of the parent (context for this relationship)

@dataclass
class TaxonomyMappings:
    """Maps from analysis context to specific relationship UUIDs"""
    # Key insight: We need to map from analysis context (segment/benefit names from analysis)
    # to the specific relationship UUID that should be used for Directus
    # This is NOT a simple name->relationship mapping since names aren't unique
    segments: Dict[str, str]  # analysis_segment_key -> relationship_id
    benefits: Dict[str, str]  # analysis_benefit_key -> relationship_id  
    conditions: Dict[str, str]  # analysis_condition_key -> relationship_id
    limits: Dict[str, str]  # analysis_limit_key -> relationship_id
    exclusions: Dict[str, str]  # analysis_exclusion_key -> relationship_id

@dataclass
class TaxonomyData:
    """Unified taxonomy data structure with relationship-based hierarchy"""
    segments: List[Dict]
    benefits: Dict[str, List[Dict]] 
    details: Dict[str, Dict[str, List[Dict]]]
    mappings: TaxonomyMappings
    relationships: Dict[str, TaxonomyRelationship]  # relationship_id -> full relationship data




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
        
        # Track all relationships by their unique IDs
        relationships = {}
        
        # Initialize mappings for this specific DCM product
        segment_relationships = {}
        benefit_relationships = {}
        condition_relationships = {}
        limit_relationships = {}
        exclusion_relationships = {}
        
        # Process taxonomy items - build the relationship hierarchy
        for item in result['taxonomy_items']:
            if item['category'] == 'product_type':
                # Process segments (direct children of product_type)
                for segment_rel in item['parent_relationships']:
                    segment_item = segment_rel['related_taxonomy_item']
                    if segment_item['category'] == 'segment_type':
                        segment_name = segment_item['taxonomy_item_name']
                        segment_rel_id = segment_rel['id']
                        
                        # Store the relationship
                        relationships[segment_rel_id] = TaxonomyRelationship(
                            relationship_id=segment_rel_id,
                            taxonomy_item_id=segment_item['id'],
                            taxonomy_item_name=segment_name,
                            category='segment_type',
                            parent_category='product_type'
                        )
                        
                        # Map segment name to its specific relationship ID for this DCM
                        segment_relationships[segment_name] = segment_rel_id
                        
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
                        
                        # Process benefits for this segment
                        segment_benefits = []
                        for benefit_rel in segment_item.get('parent_relationships', []):
                            benefit_item = benefit_rel['related_taxonomy_item']
                            if benefit_item['category'] == 'benefit_type':
                                benefit_name = benefit_item['taxonomy_item_name']
                                benefit_rel_id = benefit_rel['id']
                                
                                # Store the relationship
                                relationships[benefit_rel_id] = TaxonomyRelationship(
                                    relationship_id=benefit_rel_id,
                                    taxonomy_item_id=benefit_item['id'],
                                    taxonomy_item_name=benefit_name,
                                    category='benefit_type',
                                    parent_category='segment_type'
                                )
                                
                                # Map benefit name to its specific relationship ID for this DCM
                                benefit_relationships[benefit_name] = benefit_rel_id
                                
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
                                
                                # Process details for this benefit
                                self._process_benefit_details(
                                    benefit_item, benefit_name, segment_name, 
                                    details, relationships, 
                                    limit_relationships, condition_relationships, exclusion_relationships
                                )
                        
                        benefits[segment_name] = segment_benefits
                        
                        # Process segment-level details
                        self._process_segment_details(
                            segment_item, segment_name, relationships,
                            limit_relationships, condition_relationships, exclusion_relationships
                        )
        
        # Create properly typed mappings
        mappings = TaxonomyMappings(
            segments=segment_relationships,
            benefits=benefit_relationships,
            conditions=condition_relationships,
            limits=limit_relationships,
            exclusions=exclusion_relationships
        )
        
        # Cache and return the data
        taxonomy_data = TaxonomyData(
            segments=segments,
            benefits=benefits, 
            details=details,
            mappings=mappings,
            relationships=relationships
        )
        self._cached_data = taxonomy_data
        return taxonomy_data
    
    def _process_benefit_details(self, benefit_item: Dict, benefit_name: str, 
                            segment_name: str, details: Dict, 
                            relationships: Dict[str, TaxonomyRelationship],
                            limit_relationships: Dict[str, str], 
                            condition_relationships: Dict[str, str], 
                            exclusion_relationships: Dict[str, str]):
        """Process limits, conditions, and exclusions for a benefit"""
        benefit_key = f"{segment_name}_{benefit_name}"
        
        # Process limits
        limits = []
        for limit_rel in benefit_item.get('benefit_limits', []):
            limit_item = limit_rel['related_taxonomy_item']
            limit_name = limit_item['taxonomy_item_name']
            limit_rel_id = limit_rel['id']
            
            # Store the relationship
            relationships[limit_rel_id] = TaxonomyRelationship(
                relationship_id=limit_rel_id,
                taxonomy_item_id=limit_item['id'],
                taxonomy_item_name=limit_name,
                category='limit_type',
                parent_category='benefit_type'
            )
            
            # Map limit name to its specific relationship ID for this DCM
            limit_relationships[limit_name] = limit_rel_id
            
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
        
        details['limits'][benefit_key] = limits
        
        # Process conditions
        conditions = []
        for condition_rel in benefit_item.get('benefit_conditions', []):
            condition_item = condition_rel['related_taxonomy_item']
            condition_name = condition_item['taxonomy_item_name']
            condition_rel_id = condition_rel['id']
            
            # Store the relationship
            relationships[condition_rel_id] = TaxonomyRelationship(
                relationship_id=condition_rel_id,
                taxonomy_item_id=condition_item['id'],
                taxonomy_item_name=condition_name,
                category='condition_type',
                parent_category='benefit_type'
            )
            
            # Map condition name to its specific relationship ID for this DCM
            condition_relationships[condition_name] = condition_rel_id
            
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
        
        details['conditions'][benefit_key] = conditions
        
        # Process exclusions
        exclusions = []
        for exclusion_rel in benefit_item.get('benefit_exclusions', []):
            exclusion_item = exclusion_rel['related_taxonomy_item']
            exclusion_name = exclusion_item['taxonomy_item_name']
            exclusion_rel_id = exclusion_rel['id']
            
            # Store the relationship
            relationships[exclusion_rel_id] = TaxonomyRelationship(
                relationship_id=exclusion_rel_id,
                taxonomy_item_id=exclusion_item['id'],
                taxonomy_item_name=exclusion_name,
                category='exclusion_type',
                parent_category='benefit_type'
            )
            
            # Map exclusion name to its specific relationship ID for this DCM
            exclusion_relationships[exclusion_name] = exclusion_rel_id
            
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
        
        details['exclusions'][benefit_key] = exclusions
    
    def _process_segment_details(self, segment_item: Dict, segment_name: str, 
                           relationships: Dict[str, TaxonomyRelationship],
                           limit_relationships: Dict[str, str], 
                           condition_relationships: Dict[str, str], 
                           exclusion_relationships: Dict[str, str]):
        """Process segment-level details (conditions, limits, exclusions)"""
        
        # Process segment-level conditions
        for condition_rel in segment_item.get('segment_conditions', []):
            condition_item = condition_rel['related_taxonomy_item']
            condition_name = condition_item['taxonomy_item_name']
            condition_rel_id = condition_rel['id']
            
            # Store the relationship
            relationships[condition_rel_id] = TaxonomyRelationship(
                relationship_id=condition_rel_id,
                taxonomy_item_id=condition_item['id'],
                taxonomy_item_name=condition_name,
                category='condition_type',
                parent_category='segment_type'
            )
            
            # Map condition name to its specific relationship ID for this DCM
            condition_relationships[condition_name] = condition_rel_id
        
        # Process segment-level limits  
        for limit_rel in segment_item.get('segment_limits', []):
            limit_item = limit_rel['related_taxonomy_item']
            limit_name = limit_item['taxonomy_item_name']
            limit_rel_id = limit_rel['id']
            
            # Store the relationship
            relationships[limit_rel_id] = TaxonomyRelationship(
                relationship_id=limit_rel_id,
                taxonomy_item_id=limit_item['id'],
                taxonomy_item_name=limit_name,
                category='limit_type',
                parent_category='segment_type'
            )
            
            # Map limit name to its specific relationship ID for this DCM
            limit_relationships[limit_name] = limit_rel_id
        
        # Process segment-level exclusions
        for exclusion_rel in segment_item.get('segment_exclusions', []):
            exclusion_item = exclusion_rel['related_taxonomy_item']
            exclusion_name = exclusion_item['taxonomy_item_name']
            exclusion_rel_id = exclusion_rel['id']
            
            # Store the relationship
            relationships[exclusion_rel_id] = TaxonomyRelationship(
                relationship_id=exclusion_rel_id,
                taxonomy_item_id=exclusion_item['id'],
                taxonomy_item_name=exclusion_name,
                category='exclusion_type',
                parent_category='segment_type'
            )
            
            # Map exclusion name to its specific relationship ID for this DCM
            exclusion_relationships[exclusion_name] = exclusion_rel_id

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


def sanitize_utf8_text(text: str) -> str:
    """
    Sanitize text to ensure it's valid UTF-8 and remove problematic characters.
    
    Args:
        text: Input text that may contain invalid UTF-8 sequences
        
    Returns:
        Sanitized text safe for database storage
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
    
    return sanitized

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
            
            # Extract the typed mappings for the seeder
            self.taxonomy_mappings = taxonomy_data.mappings
            print(f"✓ Loaded taxonomy mappings: {len(self.taxonomy_mappings.segments)} segments, {len(self.taxonomy_mappings.benefits)} benefits")
            
        except Exception as e:
            print(f"Error fetching taxonomy mappings: {e}")
            print("Continuing without taxonomy mappings - items may fail to create")
            # Create empty typed mappings as fallback
            # TaxonomyMappings is defined in this same file, no import needed
            self.taxonomy_mappings = TaxonomyMappings(
                segments={}, 
                benefits={}, 
                conditions={}, 
                limits={}, 
                exclusions={}
            )

    def set_taxonomy_data(self, taxonomy_data: TaxonomyData):
        """Set pre-fetched taxonomy data from unified fetcher to avoid duplicate GraphQL calls"""
        self.taxonomy_mappings = taxonomy_data.mappings
        print(f"✓ Using pre-fetched taxonomy mappings: {len(self.taxonomy_mappings.segments)} segments, {len(self.taxonomy_mappings.benefits)} benefits")

    def _sanitize_text(self, text: str) -> str:
        """Sanitize text for UTF-8 encoding and remove problematic characters"""
        if not text:
            return ""
        
        # Ensure string type
        if not isinstance(text, str):
            text = str(text)
        
        # Remove null bytes and other problematic characters
        text = text.replace('\x00', '').replace('\r\n', '\n').replace('\r', '\n')
        
        # Ensure proper UTF-8 encoding
        try:
            # Encode and decode to ensure clean UTF-8
            text = text.encode('utf-8', errors='ignore').decode('utf-8')
        except UnicodeError:
            # Fallback for problematic strings
            text = repr(text)[1:-1]  # Use repr but strip quotes
        
        return text.strip()

    def _create_item(self, collection: str, data: Dict[str, Any]) -> str:
        """Create item in Directus collection and return its ID"""
        if self.dry_run:
            print(f"[DRY RUN] Would create {collection}: {data.get('segment_name', data.get('benefit_name', data.get('condition_name', data.get('limit_name', data.get('exclusion_name', 'Unknown')))))}")
            return str(uuid.uuid4())
        
        result = self.client.create_item(collection, data)
        return result['id']


    def create_segment(self, segment_key: str, segment_data: Dict[str, Any], product_id: str) -> str:
        """Create insurance_dcm_segment entry and return its ID"""

        print(f"\n=== CREATE_SEGMENT DEBUG ===")
        print(f"segment_key: {segment_key}")
        print(f"segment_data type: {type(segment_data)}")
        print(f"segment_data: {segment_data}")
        
        # Get taxonomy relationship ID directly from enriched analysis result
        taxonomy_rel_id = segment_data.get('taxonomy_relationship_id')
        print(f"taxonomy_rel_id from .get(): {taxonomy_rel_id}")
        
        # Also try direct attribute access
        if hasattr(segment_data, 'taxonomy_relationship_id'):
            print(f"taxonomy_relationship_id via attr: {segment_data.taxonomy_relationship_id}")
        
        # Try dict() method if available
        if hasattr(segment_data, 'dict'):
            segment_dict = segment_data.dict()
            print(f"taxonomy_relationship_id from dict(): {segment_dict.get('taxonomy_relationship_id')}")
        
        print(f"=== END DEBUG ===\n")
        
        if not taxonomy_rel_id:
            raise ValueError(f"Taxonomy relationship cannot be null for segment: {segment_data.get('item_name', segment_key)}")

        # Base data structure with UTF-8 sanitization
        data = {
            'status': 'published',
            'segment_name': self._sanitize_text(segment_data.get('item_name', segment_key)),
            'segment_description': self._sanitize_text(segment_data.get('description', '')),
            'segment_text': self._sanitize_text(segment_data.get('llm_summary', '')),
            'section_reference': self._sanitize_text(segment_data.get('section_reference', '')),
            'full_text_part': self._sanitize_text(segment_data.get('full_text_part', '')),
            'is_included': segment_data.get('is_included', False),
            'dcm_product': product_id,
            'taxonomy_item_relationship': taxonomy_rel_id
        }

        segment_id = self._create_item('insurance_dcm_segment', data)
        print(f"  ✓ Created segment: {data['segment_name']} (ID: {segment_id})")
        return segment_id

    def create_benefit(self, benefit_key: str, benefit_data: Dict[str, Any], segment_id: str, segment_name: str) -> str:
        """Create insurance_dcm_benefit entry and return its ID"""

        # Get taxonomy relationship ID directly from enriched analysis result
        taxonomy_rel_id = benefit_data.get('taxonomy_relationship_id')
        
        if not taxonomy_rel_id:
            raise ValueError(f"Taxonomy relationship cannot be null for benefit: {benefit_data.get('item_name', benefit_key)}")

        # Handle value attribute - if float convert to string, if string check 255 char limit
        raw_value = benefit_data.get('value')
        cleaned_value = None
        if raw_value and raw_value != 'N/A':
            if isinstance(raw_value, float):
                # Convert float to string and ensure it's shorter than 255 chars
                cleaned_value = str(raw_value)
                if len(cleaned_value) > 255:
                    cleaned_value = cleaned_value[:255]
            else:
                # It's a string already, only check for 255 char limit
                sanitized_value = sanitize_utf8_text(str(raw_value))
                cleaned_value = sanitized_value[:255] if len(sanitized_value) > 255 else sanitized_value

        benefit_name = benefit_data.get('item_name', benefit_key)
        
        data = {
            'status': 'published',
            'benefit_name': sanitize_utf8_text(benefit_name),
            'description': sanitize_utf8_text(benefit_data.get('description')),
            'insurance_dcm_segment': segment_id,
            'taxonomy_item_relationship': taxonomy_rel_id,
            'actual_value': cleaned_value,
            'unit': sanitize_utf8_text(benefit_data.get('unit')) if benefit_data.get('unit') != 'N/A' else None,
            'document_reference': sanitize_utf8_text(benefit_data.get('document_reference')),
            'section_reference': sanitize_utf8_text(benefit_data.get('section_reference')),
            'full_text_part': sanitize_utf8_text(benefit_data.get('full_text_part')),
            'llm_summary': sanitize_utf8_text(benefit_data.get('llm_summary')),
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

    def extract_numeric_value_from_text(self, text: str) -> float:
        """
        Extract the first numeric value from Swiss insurance text.
        
        Args:
            text: Text containing numeric values (e.g., "50'000 für Einzelversicherung, 100'000 für Familienversicherung")
            
        Returns:
            float: The first numeric value found, or None if no valid number is found
        """
        if not text or not isinstance(text, str):
            return None
            
        # Swiss number formats: 50'000, 100'000, etc.
        import re
        
        # Pattern to match Swiss number formats with apostrophes as thousands separators
        # Examples: 50'000, 100'000, 1'500'000, etc.
        swiss_pattern = r"\b(\d{1,3}(?:'\d{3})*(?:\.\d+)?)\b"
        
        # Also match regular numbers without apostrophes
        regular_pattern = r"\b(\d+(?:\.\d+)?)\b"
        
        # Try Swiss format first (with apostrophes)
        swiss_matches = re.findall(swiss_pattern, text)
        if swiss_matches:
            # Take the first match and remove apostrophes
            clean_number = swiss_matches[0].replace("'", "")
            try:
                return float(clean_number)
            except ValueError:
                pass
                
        # Fall back to regular numbers
        regular_matches = re.findall(regular_pattern, text)
        if regular_matches:
            try:
                return float(regular_matches[0])
            except ValueError:
                pass
                
        return None

    def create_detail_item(self, detail_type: str, detail_key: str, detail_data: Dict[str, Any],
                      product_id: str = None, segment_id: str = None, benefit_id: str = None,
                      segment_name: str = None, benefit_name: str = None) -> str:
        """Create condition, limit, or exclusion entry"""
        collection_map = {
            'conditions': 'insurance_dcm_condition',
            'limits': 'insurance_dcm_limit',
            'exclusions': 'insurance_dcm_exclusion'
        }

        collection = collection_map[detail_type]

        # Get taxonomy relationship ID directly from enriched analysis result
        taxonomy_rel_id = detail_data.get('taxonomy_relationship_id')
        
        if not taxonomy_rel_id:
            raise ValueError(f"Taxonomy relationship cannot be null for {detail_type[:-1]}: {detail_data.get('item_name', detail_key)}")

        # Base data structure with UTF-8 sanitization
        data = {
            'status': 'published',
            'taxonomy_item_relationship': taxonomy_rel_id,
            'document_reference': sanitize_utf8_text(detail_data.get('document_reference')),
            'section_reference': sanitize_utf8_text(detail_data.get('section_reference')),
            'full_text_part': sanitize_utf8_text(detail_data.get('full_text_part')),
            'llm_summary': sanitize_utf8_text(detail_data.get('llm_summary')),
            'extraction_date': datetime.now(timezone.utc).isoformat(),
            'validated_by_human': False
        }

        # Add specific fields and relationships based on detail type
        if detail_type == 'conditions':
            data['condition_name'] = sanitize_utf8_text(detail_data.get('item_name', detail_key))
            data['description'] = sanitize_utf8_text(detail_data.get('description'))
            if product_id:
                data['dcm_product'] = product_id
            if segment_id:
                data['insurance_dcm_segment'] = segment_id
            if benefit_id:
                data['insurance_dcm_benefit'] = benefit_id

        elif detail_type == 'limits':
            data['limit_name'] = sanitize_utf8_text(detail_data.get('item_name', detail_key))
            data['description'] = sanitize_utf8_text(detail_data.get('description'))
            
            # Enhanced value handling for Swiss number formats
            raw_value = detail_data.get('value')
            if raw_value is not None and raw_value not in ['N/A', '', ',']:
                if isinstance(raw_value, (int, float)):
                    # Use numeric value directly
                    data['limit_value'] = float(raw_value)
                else:
                    # It's a string, try smart extraction
                    text_value = str(raw_value).strip()
                    
                    # Check for unlimited keywords first
                    if text_value.lower() in ['unbegrenzt', 'unlimited', 'illimité', 'unlimited coverage']:
                        data['limit_value'] = None  # Store as NULL for unlimited
                    else:
                        # Try to extract numeric value using the new method
                        extracted_value = self.extract_numeric_value_from_text(text_value)
                        if extracted_value is not None:
                            # Successfully extracted a numeric value
                            if extracted_value.is_integer():
                                data['limit_value'] = int(extracted_value)
                            else:
                                data['limit_value'] = extracted_value
                            print(f"      ✓ Extracted numeric value: {data['limit_value']} from '{text_value[:50]}...'")
                        else:
                            # No numeric value could be extracted
                            data['limit_value'] = None
                            print(f"      ⚠ No numeric value found in '{text_value}' - stored as NULL")
            else:
                data['limit_value'] = None
                
            data['limit_unit'] = sanitize_utf8_text(detail_data.get('unit'))
            if product_id:
                data['dcm_product'] = product_id
            if segment_id:
                data['insurance_dcm_segment'] = segment_id
            if benefit_id:
                data['insurance_dcm_benefit'] = benefit_id

        elif detail_type == 'exclusions':
            data['exclusion_name'] = sanitize_utf8_text(detail_data.get('item_name', detail_key))
            data['description'] = sanitize_utf8_text(detail_data.get('description'))
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
                       segment_name: str = None, benefit_name: str = None):
        """Process conditions, limits, or exclusions"""
        for detail_item in details_data:
            for detail_key, detail_data in detail_item.items():
                if detail_data.get('is_included'):
                    self.create_detail_item(detail_type, detail_key, detail_data,
                                          product_id, segment_id, benefit_id,
                                          segment_name, benefit_name)

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

        # Process each segment
        for segment_item in segments_data:
            for segment_key, segment_data in segment_item.items():
                if not segment_data.get('is_included'):
                    continue

                print(f"\nProcessing segment: {segment_key}")
                segment_id = self.create_segment(segment_key, segment_data, product_id)
                segment_name = segment_data.get('item_name', segment_key)

                # Process segment-level details
                if 'conditions' in segment_data:
                    self.process_details(segment_data['conditions'], 'conditions',
                                       product_id=product_id, segment_id=segment_id,
                                       segment_name=segment_name)
                if 'limits' in segment_data:
                    self.process_details(segment_data['limits'], 'limits',
                                       product_id=product_id, segment_id=segment_id,
                                       segment_name=segment_name)
                if 'exclusions' in segment_data:
                    self.process_details(segment_data['exclusions'], 'exclusions',
                                       product_id=product_id, segment_id=segment_id,
                                       segment_name=segment_name)

                # Process benefits within this segment
                benefits_data = segment_data.get('benefits', [])
                for benefit_item in benefits_data:
                    for benefit_key, benefit_data in benefit_item.items():
                        if not benefit_data.get('is_included'):
                            continue

                        benefit_id = self.create_benefit(benefit_key, benefit_data, segment_id, segment_name)
                        benefit_name = benefit_data.get('item_name', benefit_key)

                        # Process benefit-level details
                        if 'conditions' in benefit_data:
                            self.process_details(benefit_data['conditions'], 'conditions',
                                               product_id=product_id, segment_id=segment_id, benefit_id=benefit_id,
                                               segment_name=segment_name, benefit_name=benefit_name)
                        if 'limits' in benefit_data:
                            self.process_details(benefit_data['limits'], 'limits',
                                               product_id=product_id, segment_id=segment_id, benefit_id=benefit_id,
                                               segment_name=segment_name, benefit_name=benefit_name)
                        if 'exclusions' in benefit_data:
                            self.process_details(benefit_data['exclusions'], 'exclusions',
                                               product_id=product_id, segment_id=segment_id, benefit_id=benefit_id,
                                               segment_name=segment_name, benefit_name=benefit_name)

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