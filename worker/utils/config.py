"""
Configuration management for the DCI Generator
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class AnalyzerConfig:
    """Centralized configuration for the document analyzer"""
    
    # Required Configuration (no defaults)
    openai_api_key: str
    directus_url: str
    directus_auth_token: str
    
    # Optional Configuration (with defaults)
    openai_model: str = "gpt-4o-mini"
    
    # Performance Configuration
    max_concurrent_requests: int = 15
    
    # Processing Configuration
    default_segment_chunks: int = 8
    default_benefit_chunks: int = 8
    default_modifier_chunks: int = 3
    
    # Debug Configuration
    debug_base_dir: str = "debug"
    
    @classmethod
    def from_environment(cls) -> 'AnalyzerConfig':
        """Create configuration from environment variables with validation"""
        
        # Required environment variables
        openai_api_key = os.getenv("OPENAI_API_KEY")
        directus_url = os.getenv("DIRECTUS_URL")
        directus_auth_token = os.getenv("DIRECTUS_AUTH_TOKEN")
        
        # Validate required variables
        missing_vars = []
        if not openai_api_key:
            missing_vars.append("OPENAI_API_KEY")
        if not directus_url:
            missing_vars.append("DIRECTUS_URL")
        if not directus_auth_token:
            missing_vars.append("DIRECTUS_AUTH_TOKEN")
            
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Optional environment variables with defaults
        openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        max_concurrent = int(os.getenv("MAX_CONCURRENT_REQUESTS", "15"))
        
        # Processing chunk sizes
        segment_chunks = int(os.getenv("DEFAULT_SEGMENT_CHUNKS", "8"))
        benefit_chunks = int(os.getenv("DEFAULT_BENEFIT_CHUNKS", "8"))
        modifier_chunks = int(os.getenv("DEFAULT_MODIFIER_CHUNKS", "3"))
        
        # Debug configuration
        debug_base_dir = os.getenv("DEBUG_BASE_DIR", "debug")
        
        return cls(
            openai_api_key=openai_api_key,
            openai_model=openai_model,
            directus_url=directus_url,
            directus_auth_token=directus_auth_token,
            max_concurrent_requests=max_concurrent,
            default_segment_chunks=segment_chunks,
            default_benefit_chunks=benefit_chunks,
            default_modifier_chunks=modifier_chunks,
            debug_base_dir=debug_base_dir
        )
    
    @property
    def graphql_url(self) -> str:
        """Get the GraphQL endpoint URL"""
        return f"{self.directus_url}/graphql"
    
    def validate(self) -> None:
        """Validate configuration values"""
        if self.max_concurrent_requests < 1 or self.max_concurrent_requests > 50:
            raise ValueError("max_concurrent_requests must be between 1 and 50")
            
        if any(chunks < 1 for chunks in [self.default_segment_chunks, self.default_benefit_chunks, self.default_modifier_chunks]):
            raise ValueError("All chunk sizes must be at least 1")
            
        if not self.directus_url.startswith(("http://", "https://")):
            raise ValueError("directus_url must be a valid HTTP/HTTPS URL")


# Global configuration instance
_config: Optional[AnalyzerConfig] = None

def get_config() -> AnalyzerConfig:
    """Get the global configuration instance, loading from environment if needed"""
    global _config
    if _config is None:
        _config = AnalyzerConfig.from_environment()
        _config.validate()
    return _config

def reset_config() -> None:
    """Reset the global configuration (useful for testing)"""
    global _config
    _config = None