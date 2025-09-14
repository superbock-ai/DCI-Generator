"""
Directus Data Seeder Module for Analysis Results

This module provides functions to insert insurance analysis results into Directus
while maintaining hierarchical relationships between:
- dcm_product (existing insurance product)
- insurance_dcm_segment
- insurance_dcm_benefit
- insurance_dcm_condition, insurance_dcm_limit, insurance_dcm_exclusion

The module preserves the hierarchical structure through proper relationship management.

Usage:
    from directus_seeder import seed_to_directus, cleanup_seeded_data

    # Seed analysis results
    success = seed_to_directus(analysis_results, product_id, dry_run=False)

    # Clean up seeded data
    success = cleanup_seeded_data()
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


class DirectusSeeder:
    """Main seeder class for inserting analysis results into Directus"""

    def __init__(self, client: DirectusClient, dry_run: bool = False):
        self.client = client
        self.dry_run = dry_run
        self.taxonomy_mappings = {}

    def fetch_taxonomy_mappings(self, dcm_id: str, graphql_url: str, auth_token: str):
        """Fetch taxonomy item mappings from GraphQL endpoint"""
        print("Fetching taxonomy mappings...")

        transport = RequestsHTTPTransport(
            url=graphql_url,
            headers={'Authorization': f'Bearer {auth_token}'}
        )
        client = Client(transport=transport)

        # Load the GraphQL query
        query_file = os.path.join(os.path.dirname(__file__), 'graphql', 'GetCompleteTaxonomyHierarchy.graphql')
        with open(query_file, 'r') as f:
            query_text = f.read()

        query = gql(query_text)

        try:
            result = client.execute(query, variable_values={'dcm_id': dcm_id})

            # Build mappings for segments, benefits, and detail types
            mappings = {
                'segments': {},
                'benefits': {},
                'conditions': {},
                'limits': {},
                'exclusions': {}
            }

            # Process taxonomy items
            for product_type in result.get('taxonomy_items', []):
                for segment_rel in product_type.get('parent_relationships', []):
                    segment_item = segment_rel.get('related_taxonomy_item', {})
                    segment_name = segment_item.get('taxonomy_item_name', '')

                    # Map segment names to taxonomy relationship IDs
                    mappings['segments'][segment_name] = segment_rel.get('id')

                    # Process benefits within segments
                    for benefit_rel in segment_item.get('parent_relationships', []):
                        benefit_item = benefit_rel.get('related_taxonomy_item', {})
                        benefit_name = benefit_item.get('taxonomy_item_name', '')

                        # Create compound key for benefit mapping
                        benefit_key = f"{segment_name}_{benefit_name}"
                        mappings['benefits'][benefit_key] = benefit_rel.get('id')

                        # Process benefit-level details
                        for condition_rel in benefit_item.get('benefit_conditions', []):
                            condition_item = condition_rel.get('related_taxonomy_item', {})
                            condition_name = condition_item.get('taxonomy_item_name', '')
                            condition_key = f"{segment_name}_{benefit_name}_{condition_name}"
                            mappings['conditions'][condition_key] = condition_rel.get('id')

                        for limit_rel in benefit_item.get('benefit_limits', []):
                            limit_item = limit_rel.get('related_taxonomy_item', {})
                            limit_name = limit_item.get('taxonomy_item_name', '')
                            limit_key = f"{segment_name}_{benefit_name}_{limit_name}"
                            mappings['limits'][limit_key] = limit_rel.get('id')

                        for exclusion_rel in benefit_item.get('benefit_exclusions', []):
                            exclusion_item = exclusion_rel.get('related_taxonomy_item', {})
                            exclusion_name = exclusion_item.get('taxonomy_item_name', '')
                            exclusion_key = f"{segment_name}_{benefit_name}_{exclusion_name}"
                            mappings['exclusions'][exclusion_key] = exclusion_rel.get('id')

                    # Process segment-level details
                    for condition_rel in segment_item.get('segment_conditions', []):
                        condition_item = condition_rel.get('related_taxonomy_item', {})
                        condition_name = condition_item.get('taxonomy_item_name', '')
                        condition_key = f"{segment_name}_{condition_name}"
                        mappings['conditions'][condition_key] = condition_rel.get('id')

                    for limit_rel in segment_item.get('segment_limits', []):
                        limit_item = limit_rel.get('related_taxonomy_item', {})
                        limit_name = limit_item.get('taxonomy_item_name', '')
                        limit_key = f"{segment_name}_{limit_name}"
                        mappings['limits'][limit_key] = limit_rel.get('id')

                    for exclusion_rel in segment_item.get('segment_exclusions', []):
                        exclusion_item = exclusion_rel.get('related_taxonomy_item', {})
                        exclusion_name = exclusion_item.get('taxonomy_item_name', '')
                        exclusion_key = f"{segment_name}_{exclusion_name}"
                        mappings['exclusions'][exclusion_key] = exclusion_rel.get('id')

            self.taxonomy_mappings = mappings
            print(f"✓ Loaded taxonomy mappings: {len(mappings['segments'])} segments, {len(mappings['benefits'])} benefits")

        except Exception as e:
            print(f"Error fetching taxonomy mappings: {e}")
            print("Continuing without taxonomy mappings - items may fail to create")
            self.taxonomy_mappings = {'segments': {}, 'benefits': {}, 'conditions': {}, 'limits': {}, 'exclusions': {}}


    def create_segment(self, segment_key: str, segment_data: Dict[str, Any], product_id: str) -> str:
        """Create insurance_dcm_segment entry and return its ID"""

        # Get taxonomy relationship ID for this segment
        segment_name = segment_data.get('item_name', segment_key)
        taxonomy_rel_id = self.taxonomy_mappings.get('segments', {}).get(segment_name)

        if not taxonomy_rel_id:
            print(f"  ⚠ No taxonomy mapping found for segment: {segment_name}")
            # Try to find by key if item_name lookup failed
            taxonomy_rel_id = self.taxonomy_mappings.get('segments', {}).get(segment_key)

        data = {
            'status': 'published',
            'segment_name': segment_name,
            'description': segment_data.get('description'),
            'dcm_product': product_id,
            'taxonomy_item_relationship': taxonomy_rel_id,
            'document_reference': segment_data.get('document_reference'),
            'section_reference': segment_data.get('section_reference'),
            'full_text_part': segment_data.get('full_text_part'),
            'llm_summary': segment_data.get('llm_summary'),
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

    def create_benefit(self, benefit_key: str, benefit_data: Dict[str, Any], segment_id: str, segment_name: str) -> str:
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

        # Clean numeric values for benefits too
        raw_value = benefit_data.get('value')
        cleaned_value = None
        if raw_value and raw_value != 'N/A':
            try:
                clean_str = str(raw_value).replace("'", "").replace(",", "")
                numeric_value = float(clean_str)
                cleaned_value = int(numeric_value) if numeric_value.is_integer() else numeric_value
            except ValueError:
                cleaned_value = raw_value

        data = {
            'status': 'published',
            'benefit_name': benefit_name,
            'description': benefit_data.get('description'),
            'insurance_dcm_segment': segment_id,
            'taxonomy_item_relationship': taxonomy_rel_id,
            'actual_value': cleaned_value,
            'unit': benefit_data.get('unit') if benefit_data.get('unit') != 'N/A' else None,
            'document_reference': benefit_data.get('document_reference'),
            'section_reference': benefit_data.get('section_reference'),
            'full_text_part': benefit_data.get('full_text_part'),
            'llm_summary': benefit_data.get('llm_summary'),
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
                          segment_name: str = None, benefit_name: str = None) -> str:
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

        # Base data structure
        data = {
            'status': 'published',
            'taxonomy_item_relationship': taxonomy_rel_id,
            'document_reference': detail_data.get('document_reference'),
            'section_reference': detail_data.get('section_reference'),
            'full_text_part': detail_data.get('full_text_part'),
            'llm_summary': detail_data.get('llm_summary'),
            'extraction_date': datetime.now(timezone.utc).isoformat(),
            'validated_by_human': False
        }

        # Add specific fields and relationships based on detail type
        if detail_type == 'conditions':
            data['condition_name'] = detail_data.get('item_name', detail_key)
            data['description'] = detail_data.get('description')
            if product_id:
                data['dcm_product'] = product_id
            if segment_id:
                data['insurance_dcm_segment'] = segment_id
            if benefit_id:
                data['insurance_dcm_benefit'] = benefit_id

        elif detail_type == 'limits':
            data['limit_name'] = detail_data.get('item_name', detail_key)
            data['description'] = detail_data.get('description')
            # Clean numeric values - remove apostrophes and convert to proper format
            raw_value = detail_data.get('value')
            if raw_value and raw_value not in ['N/A', '', ',']:
                # Remove Swiss number formatting (apostrophes) and try to convert to number
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
                    text_value = str(raw_value).strip()
                    if text_value and text_value not in ['N/A', ',']:
                        # For text values like "Unbegrenzt" (unlimited), store as NULL since it's a numeric field
                        # Or we could store a very large number to represent unlimited
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
            data['limit_unit'] = detail_data.get('unit')
            if product_id:
                data['dcm_product'] = product_id
            if segment_id:
                data['insurance_dcm_segment'] = segment_id
            if benefit_id:
                data['insurance_dcm_benefit'] = benefit_id

        elif detail_type == 'exclusions':
            data['exclusion_name'] = detail_data.get('item_name', detail_key)
            data['description'] = detail_data.get('description')
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

    def seed_analysis_results(self, analysis_results: Dict[str, Any], product_id: str, graphql_url: str, auth_token: str):
        """Main method to seed all analysis results"""
        print(f"Starting to seed Generali analysis results {'(DRY RUN)' if self.dry_run else ''}")
        print("=" * 60)

        # Get DCM ID from existing product and fetch taxonomy mappings
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
                self.fetch_taxonomy_mappings(dcm_id, graphql_url, auth_token)
            except Exception as e:
                print(f"Error fetching product info: {e}")
                return
        else:
            print(f"[DRY RUN] Would use existing dcm_product: {product_id}")
            print("[DRY RUN] Skipping taxonomy mapping fetch")

        # Get the generali data
        generali_data = analysis_results.get('generali', {})
        segments_data = generali_data.get('segments', [])

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


def load_analysis_results(file_path: str) -> Dict[str, Any]:
    """Load analysis results from JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Analysis results file not found: {file_path}")
        raise FileNotFoundError(f"Analysis results file not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in analysis results: {e}")
        raise ValueError(f"Invalid JSON in analysis results: {e}")


def seed_to_directus(analysis_results: Dict[str, Any], product_id: str, dry_run: bool = False) -> bool:
    """
    Seed analysis results to Directus under an existing dcm_product.
    
    Args:
        analysis_results: The analysis results dictionary from main.py
        product_id: Existing dcm_product ID to seed data under
        dry_run: If True, only show what would be inserted without actual insertion
    
    Returns:
        bool: True if seeding was successful, False otherwise
    """
    # Get configuration from environment
    graphql_url = os.getenv('GRAPHQL_URL')
    auth_token = os.getenv('GRAPHQL_AUTH_TOKEN')

    if not graphql_url or not auth_token:
        print("Missing GRAPHQL_URL or GRAPHQL_AUTH_TOKEN environment variables")
        return False

    # Convert GraphQL URL to Directus API URL
    directus_url = graphql_url.replace('/graphql', '')

    config = DirectusConfig(url=directus_url, auth_token=auth_token)
    client = DirectusClient(config)
    seeder = DirectusSeeder(client, dry_run=dry_run)

    try:
        seeder.seed_analysis_results(analysis_results, product_id, graphql_url, auth_token)
        return True
    except Exception as e:
        print(f"Error during seeding: {e}")
        return False


def cleanup_seeded_data(product_id: str) -> bool:
    """
    Clean up all data associated with a dcm_product from Directus.
    
    Args:
        product_id: The dcm_product UUID to clean up all associated data for
        
    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    # Get configuration from environment
    graphql_url = os.getenv('GRAPHQL_URL')
    auth_token = os.getenv('GRAPHQL_AUTH_TOKEN')

    if not graphql_url or not auth_token:
        print("Missing GRAPHQL_URL or GRAPHQL_AUTH_TOKEN environment variables")
        return False

    # Convert GraphQL URL to Directus API URL
    directus_url = graphql_url.replace('/graphql', '')

    config = DirectusConfig(url=directus_url, auth_token=auth_token)
    client = DirectusClient(config)
    seeder = DirectusSeeder(client, dry_run=False)

    try:
        return seeder.cleanup_by_product_id(product_id)
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return False


# Module can be imported and used via seed_to_directus() and cleanup_seeded_data() functions