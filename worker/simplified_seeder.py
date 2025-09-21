"""
SimplifiedSeeder - Handles Directus seeding for DCI extraction worker results
"""

import os
from typing import Dict, Any
from directus_tools import DirectusClient, DirectusConfig, DirectusSeeder


class SimplifiedSeeder:
    """Handles seeding and cleanup of analysis results to/from Directus"""
    
    def __init__(self):
        """Initialize the seeder with environment configuration"""
        self.directus_url = os.getenv('DIRECTUS_URL')
        self.auth_token = os.getenv('DIRECTUS_AUTH_TOKEN')
        
        if not self.directus_url or not self.auth_token:
            raise ValueError("Missing DIRECTUS_URL or DIRECTUS_AUTH_TOKEN environment variables")
    
    def seed_analysis_results(self, analysis_results: Dict[str, Any], product_id: str, 
                             dry_run: bool = False, taxonomy_data=None) -> bool:
        """
        Seed analysis results to Directus under an existing dcm_product.
        
        Args:
            analysis_results: The analysis results dictionary from dci_extraction_worker
            product_id: Existing dcm_product ID to seed data under
            dry_run: If True, only show what would be inserted without actual insertion
            taxonomy_data: Optional pre-fetched taxonomy data to avoid duplicate GraphQL calls
        
        Returns:
            bool: True if seeding was successful, False otherwise
        """
        try:
            config = DirectusConfig(url=self.directus_url, auth_token=self.auth_token)
            client = DirectusClient(config)
            seeder = DirectusSeeder(client, dry_run=dry_run)
            
            graphql_url = self.directus_url + '/graphql'
            
            seeder.seed_analysis_results(analysis_results, product_id, graphql_url, 
                                       self.auth_token, taxonomy_data)
            return True
        except Exception as e:
            print(f"Error during seeding: {e}")
            return False
    
    def cleanup_product_data(self, product_id: str) -> bool:
        """
        Clean up all seeded analysis data for a product from Directus.
        
        Args:
            product_id: The product ID to clean up data for
            
        Returns:
            bool: True if cleanup was successful, False otherwise
        """
        try:
            config = DirectusConfig(url=self.directus_url, auth_token=self.auth_token)
            client = DirectusClient(config)
            seeder = DirectusSeeder(client)
            
            seeder.cleanup_product_data(product_id)
            return True
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return False


# Convenience functions for backward compatibility
def seed_to_directus(analysis_results: Dict[str, Any], product_id: str, dry_run: bool = False, taxonomy_data=None) -> bool:
    """
    Convenience function for seeding analysis results to Directus.
    
    Args:
        analysis_results: The analysis results dictionary from dci_extraction_worker
        product_id: Existing dcm_product ID to seed data under
        dry_run: If True, only show what would be inserted without actual insertion
        taxonomy_data: Optional pre-fetched taxonomy data to avoid duplicate GraphQL calls
    
    Returns:
        bool: True if seeding was successful, False otherwise
    """
    try:
        seeder = SimplifiedSeeder()
        return seeder.seed_analysis_results(analysis_results, product_id, dry_run, taxonomy_data)
    except Exception as e:
        print(f"Error initializing seeder: {e}")
        return False


def cleanup_seeded_data(product_id: str) -> bool:
    """
    Convenience function for cleaning up seeded data from Directus.
    
    Args:
        product_id: The product ID to clean up data for
        
    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    try:
        seeder = SimplifiedSeeder()
        return seeder.cleanup_product_data(product_id)
    except Exception as e:
        print(f"Error initializing seeder: {e}")
        return False