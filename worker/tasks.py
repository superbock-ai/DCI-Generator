"""
Celery tasks for DCI Generator worker.
"""

import os
import json
import asyncio
from typing import Dict, Any, Optional
from celery_app import app
from simplified_seeder import seed_to_directus, cleanup_seeded_data
from directus_tools import DirectusConfig, DirectusClient


@app.task(bind=True, name='dci_worker.analyze_document')
def analyze_document_task(self, **kwargs) -> Dict[str, Any]:
    """
    Celery task for analyzing insurance documents using the simplified worker.

    Args:
        product_id (str): Product ID from Directus to analyze
        export (bool): Export results to JSON file (default: False)
        seed_directus (bool): Seed analysis results to Directus (default: False)
        dry_run_directus (bool): Dry run mode for Directus seeding (default: False)

    Returns:
        Dict containing analysis results and status
    """

    # Extract parameters with defaults
    product_id = kwargs.get('product_id')
    if not product_id:
        return {
            'success': False,
            'error': 'product_id is required',
            'num_segments': 0,
            'num_benefits': 0,
            'num_modifiers': 0,
            'seeding_success': False
        }

    export = kwargs.get('export', False)
    seed_directus = kwargs.get('seed_directus', False)
    dry_run_directus = kwargs.get('dry_run_directus', False)
    debug = kwargs.get('debug', False)

    print(f"Starting simplified analysis task for product: {product_id}")
    print(f"Task ID: {self.request.id}")
    print(f"Parameters: export={export}, seed={seed_directus}, debug={debug}")

    try:
        # Set task status to processing
        self.update_state(state='PROCESSING', meta={'status': 'Initializing analysis'})

        # Validate environment variables
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY environment variable is not set")

        if not os.getenv("DIRECTUS_AUTH_TOKEN"):
            raise ValueError("DIRECTUS_AUTH_TOKEN environment variable is not set")

        # Fetch product from Directus
        print(f"Fetching product from Directus: {product_id}")
        self.update_state(state='PROCESSING', meta={'status': 'Fetching product data'})

        directus_url = os.getenv('DIRECTUS_URL')
        directus_token = os.getenv('DIRECTUS_AUTH_TOKEN')
        if not directus_url or not directus_token:
            raise ValueError("Missing DIRECTUS_URL or DIRECTUS_AUTH_TOKEN environment variables")

        config = DirectusConfig(url=directus_url, auth_token=directus_token)
        client = DirectusClient(config)

        # Fetch the product
        product_items = client.get_items('dcm_product', {'filter[id][_eq]': product_id})
        if not product_items:
            raise ValueError(f"Product with ID {product_id} not found")

        product = product_items[0]
        document_text = product.get('full_text_part')
        if not document_text:
            raise ValueError(f"Product {product_id} has no full_text_part")

        dcm_id = product.get('domain_context_model')
        if not dcm_id:
            raise ValueError(f"Product {product_id} has no domain_context_model")

        product_name = product.get('product_name', product_id)
        print(f"✓ Found product: {product_name}")
        print(f"✓ Using DCM ID: {dcm_id}")
        print(f"✓ Document text length: {len(document_text)} characters")

        self.update_state(state='PROCESSING', meta={'status': 'Setting up analyzer'})

        # Initialize simplified analyzer
        from dci_extraction_worker import SimplifiedDocumentAnalyzer
        analyzer = SimplifiedDocumentAnalyzer(dcm_id=dcm_id, product_id=product_id)

        # Fetch taxonomy data
        print("Fetching taxonomy data from GraphQL endpoint...")
        self.update_state(state='PROCESSING', meta={'status': 'Fetching taxonomy data'})
        analyzer.fetch_taxonomy_data()

        # Analyze the document
        print("Starting document analysis...")
        self.update_state(state='PROCESSING', meta={'status': 'Analyzing document'})

        # Run async analysis
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            results_dict = loop.run_until_complete(analyzer.analyze_document(document_text, debug=debug))
        finally:
            loop.close()

        # Count results from the simplified structure
        num_segments = len(results_dict.get("segments", {}))
        num_benefits = len(results_dict.get("benefits", {}))
        num_modifiers = len(results_dict.get("modifiers", {}))

        print(f"Analysis completed: {num_segments} segments, {num_benefits} benefits, {num_modifiers} modifiers")

        # Export results if requested
        if export:
            print("Exporting results to JSON...")
            export_filename = f"exports/{product_id}_analysis_results.json"
            os.makedirs("exports", exist_ok=True)
            with open(export_filename, 'w', encoding='utf-8') as f:
                json.dump(results_dict, f, indent=2, ensure_ascii=False)
            print(f"✓ Results exported to: {export_filename}")

        # Seed to Directus if requested
        seeding_success = True
        if seed_directus:
            print("\n" + "=" * 60)
            print("Seeding results to Directus...")
            print("=" * 60)

            self.update_state(state='PROCESSING', meta={'status': 'Seeding to Directus'})

            # Get hierarchical results from analyzer (preserves proper parent-child relationships)
            converted_results = analyzer.get_hierarchical_results_for_seeding()
            
            seeder_format = {product_id: converted_results}

            seeding_success = seed_to_directus(
                analysis_results=seeder_format,
                product_id=product_id,
                dry_run=dry_run_directus,
                taxonomy_data=None  # Simplified worker doesn't need this format
            )

            if seeding_success:
                print("✓ Successfully seeded results to Directus!")
            else:
                print("✗ Failed to seed results to Directus")

        print("Analysis completed successfully!")

        return {
            'success': True,
            'num_segments': num_segments,
            'num_benefits': num_benefits,
            'num_modifiers': num_modifiers,
            'seeding_success': seeding_success,
            'product_id': product_id,
            'product_name': product_name
        }

    except Exception as e:
        error_msg = str(e)
        print(f"Analysis task failed: {error_msg}")

        # Properly format exception for Celery backend
        self.update_state(
            state='FAILURE',
            meta={
                'error': error_msg,
                'status': 'Analysis failed',
                'exc_type': type(e).__name__,
                'exc_message': str(e)
            }
        )

        return {
            'success': False,
            'error': error_msg,
            'num_segments': 0,
            'num_benefits': 0,
            'num_modifiers': 0,
            'seeding_success': False,
            'product_id': product_id
        }


@app.task(bind=True, name='dci_worker.cleanup_product')
def cleanup_product_task(self, product_id: str) -> Dict[str, Any]:
    """
    Celery task for cleaning up seeded data from Directus.

    Args:
        product_id (str): Product ID to clean up

    Returns:
        Dict containing cleanup status
    """
    print(f"Starting cleanup task for product: {product_id}")
    print(f"Task ID: {self.request.id}")

    try:
        self.update_state(state='PROCESSING', meta={'status': 'Cleaning up product data'})

        print("=" * 60)
        print(f"Cleaning up all data for product: {product_id}")
        print("=" * 60)

        success = cleanup_seeded_data(product_id)

        if success:
            print("✓ Successfully cleaned up all data from Directus!")
            return {
                'success': True,
                'product_id': product_id,
                'message': 'Product data cleaned up successfully'
            }
        else:
            print("✗ Failed to clean up data from Directus")
            return {
                'success': False,
                'product_id': product_id,
                'error': 'Failed to clean up product data'
            }

    except Exception as e:
        error_msg = str(e)
        print(f"Cleanup task failed: {error_msg}")

        # Properly format exception for Celery backend
        self.update_state(
            state='FAILURE',
            meta={
                'error': error_msg,
                'status': 'Cleanup failed',
                'exc_type': type(e).__name__,
                'exc_message': str(e)
            }
        )

        return {
            'success': False,
            'error': error_msg,
            'product_id': product_id
        }
