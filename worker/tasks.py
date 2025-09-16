"""
Celery tasks for DCI Generator worker.
"""

import os
import asyncio
from typing import Dict, Any, Optional
from celery_app import app
from main import DocumentAnalyzer, convert_pydantic_to_dict, seed_to_directus, cleanup_seeded_data
from directus_tools import DirectusConfig, DirectusClient


@app.task(bind=True, name='dci_worker.analyze_document')
def analyze_document_task(self, **kwargs) -> Dict[str, Any]:
    """
    Celery task for analyzing insurance documents.

    Args:
        product_id (str): Product ID from Directus to analyze
        export (bool): Export results to JSON file (default: False)
        detailed (bool): Show detailed results (default: False)
        no_cache (bool): Disable caching for this run (default: False)
        segment_chunks (int): Number of segments to process in parallel per chunk (default: 8)
        benefit_chunks (int): Number of benefits to process in parallel per chunk (default: 8)
        detail_chunks (int): Number of details to process in parallel per chunk (default: 3)
        debug (bool): Enable debug mode (default: False)
        debug_clean (bool): Delete existing debug files before running (default: False)
        debug_from (str): Force re-run from specific tier (default: None)
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
            'num_limits': 0,
            'seeding_success': False
        }

    export = kwargs.get('export', False)
    detailed = kwargs.get('detailed', False)
    no_cache = kwargs.get('no_cache', False)
    segment_chunks = kwargs.get('segment_chunks', 8)
    benefit_chunks = kwargs.get('benefit_chunks', 8)
    detail_chunks = kwargs.get('detail_chunks', 3)
    debug = kwargs.get('debug', False)
    debug_clean = kwargs.get('debug_clean', False)
    debug_from = kwargs.get('debug_from')
    seed_directus = kwargs.get('seed_directus', False)
    dry_run_directus = kwargs.get('dry_run_directus', False)

    print(f"Starting analysis task for product: {product_id}")
    print(f"Task ID: {self.request.id}")
    print(f"Parameters: export={export}, detailed={detailed}, debug={debug}, seed={seed_directus}")

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

        # Handle debug flags
        document_name = product_id

        if debug_clean:
            print("🗑️ Cleaning all debug files...")
            # Import the clean_debug_files function from main
            from main import clean_debug_files
            clean_debug_files(document_name)

        if debug_from:
            print(f"🔄 Cleaning debug files from {debug_from} onwards...")
            from main import clean_debug_files
            clean_debug_files(document_name, debug_from)

        # Disable cache if requested
        if no_cache:
            print("Caching disabled for this run")
            from langchain.globals import set_llm_cache
            set_llm_cache(None)

        self.update_state(state='PROCESSING', meta={'status': 'Setting up analyzer'})

        # Initialize analyzer
        analyzer = DocumentAnalyzer(
            segment_chunk_size=segment_chunks,
            benefit_chunk_size=benefit_chunks,
            detail_chunk_size=detail_chunks,
            debug_mode=debug,
            dcm_id=dcm_id
        )

        # Fetch taxonomy segments, benefits, and details from GraphQL
        print("Fetching segment taxonomy from GraphQL endpoint...")
        self.update_state(state='PROCESSING', meta={'status': 'Fetching taxonomy data'})

        # Run async code in event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            analyzer.segments = loop.run_until_complete(analyzer.fetch_taxonomy_segments())

            print(f"Found {len(analyzer.segments)} segment types:")
            total_benefits = 0
            total_details = 0
            for segment in analyzer.segments:
                segment_benefits = len(analyzer.benefits.get(segment['name'], []))
                total_benefits += segment_benefits

                # Count details for this segment
                segment_details = 0
                for benefit in analyzer.benefits.get(segment['name'], []):
                    benefit_key = f"{segment['name']}_{benefit['name']}"
                    for detail_type in ['limits', 'conditions', 'exclusions']:
                        segment_details += len(analyzer.details[detail_type].get(benefit_key, []))
                total_details += segment_details

                print(f"  - {segment['name']}: {segment['description']} ({segment_benefits} benefits, {segment_details} details)")

            print(f"Total available for analysis: {total_benefits} benefits, {total_details} details")

            # Setup analysis chains
            analyzer.setup_analysis_chains()

            # Analyze the document
            print("Starting document analysis...")
            self.update_state(state='PROCESSING', meta={'status': 'Analyzing document'})

            results = loop.run_until_complete(analyzer.analyze_document_text(document_text, document_name))

        finally:
            loop.close()

        # Count results
        num_segments = len(results.segments)
        num_benefits = sum(len(segment.benefits) for segment in results.segments)
        num_limits = sum(
            len(benefit.limits) + len(benefit.conditions) + len(benefit.exclusions)
            for segment in results.segments
            for benefit in segment.benefits
        )

        print(f"Analysis completed: {num_segments} segments, {num_benefits} benefits, {num_limits} details")

        # Export results if requested
        if export:
            print("Exporting results to JSON...")
            analyzer.export_results(results, document_name)

        # Show detailed results if requested
        if detailed:
            analyzer.print_detailed_results(results, document_name)

        # Seed to Directus if requested
        seeding_success = True
        if seed_directus:
            print("\n" + "=" * 60)
            print("Seeding results to Directus...")
            print("=" * 60)

            self.update_state(state='PROCESSING', meta={'status': 'Seeding to Directus'})

            # Convert results to the format expected by directus_seeder
            serialized_results = convert_pydantic_to_dict(results)
            seeder_format = {document_name: serialized_results}

            seeding_success = seed_to_directus(
                analysis_results=seeder_format,
                product_id=product_id,
                dry_run=dry_run_directus,
                taxonomy_data=analyzer.taxonomy_data
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
            'num_limits': num_limits,
            'seeding_success': seeding_success,
            'product_id': product_id,
            'product_name': product_name
        }

    except Exception as e:
        error_msg = str(e)
        print(f"Analysis task failed: {error_msg}")

        self.update_state(
            state='FAILURE',
            meta={'error': error_msg, 'status': 'Analysis failed'}
        )

        return {
            'success': False,
            'error': error_msg,
            'num_segments': 0,
            'num_benefits': 0,
            'num_limits': 0,
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

        self.update_state(
            state='FAILURE',
            meta={'error': error_msg, 'status': 'Cleanup failed'}
        )

        return {
            'success': False,
            'error': error_msg,
            'product_id': product_id
        }