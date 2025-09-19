import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Coroutine
from datetime import datetime

from dotenv import load_dotenv
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel
from langchain_core.globals import set_llm_cache
from langchain_core.caches import InMemoryCache
from pydantic import BaseModel, Field
from dataclasses import dataclass, field
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
from openai import RateLimitError
import re
import math

# Import Directus seeding functionality
from directus_tools import DirectusClient, DirectusConfig, DirectusSeeder, UnifiedTaxonomyFetcher

# Load environment variables from .env file
load_dotenv()

# Set up in-memory caching for LangChain
set_llm_cache(InMemoryCache())


import hashlib

def convert_pydantic_to_dict(obj):
    """Recursively convert Pydantic models to dictionaries."""
    if hasattr(obj, 'dict'):  # Pydantic model
        return obj.dict()
    elif isinstance(obj, dict):
        return {k: convert_pydantic_to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_pydantic_to_dict(item) for item in obj]
    else:
        return obj


def cleanup_seeded_data(product_id: str) -> bool:
    """
    Clean up all data associated with a dcm_product from Directus.
    
    Args:
        product_id: The dcm_product UUID to clean up all associated data for
        
    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    # Get configuration from environment
    directus_url = os.getenv('DIRECTUS_URL')
    auth_token = os.getenv('DIRECTUS_AUTH_TOKEN')

    if not directus_url or not auth_token:
        print("Missing DIRECTUS_URL or DIRECTUS_AUTH_TOKEN environment variables")
        return False

    config = DirectusConfig(url=directus_url, auth_token=auth_token)
    client = DirectusClient(config)
    seeder = DirectusSeeder(client, dry_run=False)

    try:
        return seeder.cleanup_by_product_id(product_id)
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return False

def seed_to_directus(analysis_results: Dict[str, Any], product_id: str, dry_run: bool = False, taxonomy_data=None) -> bool:
    """
    Seed analysis results to Directus under an existing dcm_product.
    
    Args:
        analysis_results: The analysis results dictionary from main.py
        product_id: Existing dcm_product ID to seed data under
        dry_run: If True, only show what would be inserted without actual insertion
        taxonomy_data: Optional pre-fetched taxonomy data to avoid duplicate GraphQL calls
    
    Returns:
        bool: True if seeding was successful, False otherwise
    """
    # Get configuration from environment
    directus_url = os.getenv('DIRECTUS_URL')
    auth_token = os.getenv('DIRECTUS_AUTH_TOKEN')

    if not directus_url or not auth_token:
        print("Missing DIRECTUS_URL or DIRECTUS_AUTH_TOKEN environment variables")
        return False

    config = DirectusConfig(url=directus_url, auth_token=auth_token)
    client = DirectusClient(config)
    seeder = DirectusSeeder(client, dry_run=dry_run)
    
    graphql_url = directus_url + '/graphql'

    try:
        seeder.seed_analysis_results(analysis_results, product_id, graphql_url, auth_token, taxonomy_data)
        return True
    except Exception as e:
        print(f"Error during seeding: {e}")
        return False


def parse_openai_wait_time(error_message: str):
    """
    Parse the wait time from OpenAI's rate limit error message.

    Args:
        error_message: The error message from OpenAI API

    Returns:
        Wait time in seconds, ceiled to next full second. Returns None if parsing fails.
    """
    # Patterns to match OpenAI's rate limit messages
    patterns = [
        r"try again in (\d+\.?\d*)\s*s",
        r"try again in (\d+\.?\d*)\s*seconds",
        r"Please try again in (\d+\.?\d*)\s*s",
        r"Please try again in (\d+\.?\d*)\s*seconds",
        r"reset in (\d+\.?\d*)\s*s",
        r"reset in (\d+\.?\d*)\s*seconds"
    ]

    for pattern in patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match:
            wait_time = float(match.group(1))
            # Always ceil to next full second as requested
            ceiled_time = math.ceil(wait_time)
            print(f"    Parsed wait time: {wait_time}s → using {ceiled_time}s (ceiled)")
            return ceiled_time

    return None


def create_smart_wait_strategy():
    """
    Create a wait strategy that tries to parse OpenAI's suggested wait time,
    falling back to exponential backoff if parsing fails.
    """
    def smart_wait(retry_state):
        # Try to parse the wait time from the exception
        if retry_state.outcome and retry_state.outcome.exception():
            exception = retry_state.outcome.exception()
            error_message = str(exception)

            # Try to parse OpenAI's suggested wait time
            parsed_wait = parse_openai_wait_time(error_message)
            if parsed_wait is not None:
                return parsed_wait

        # Fallback to exponential backoff with random jitter
        exponential_wait = wait_random_exponential(multiplier=1, max=60)
        fallback_time = exponential_wait(retry_state)
        print(f"    Using fallback exponential backoff: {fallback_time:.1f}s")
        return fallback_time

    return smart_wait


def get_debug_filename(document_name: str, tier: str) -> str:
    """Get debug filename for a specific document and analysis tier."""
    return f"debug/{document_name}_{tier}.debug.json"


def save_debug_results(document_name: str, tier: str, results: dict, chunk_sizes: dict):
    """Save analysis results to debug file with chunk size metadata."""
    filename = get_debug_filename(document_name, tier)

    debug_data = {
        "tier": tier,
        "chunk_sizes": chunk_sizes,
        "results": {}
    }

    # Convert results to dictionaries with taxonomy_relationship_id as key
    if results:  # Only process if results is not empty
        for key, result in results.items():
            if hasattr(result, 'dict'):  # EnrichedAnalysisResult or Pydantic model
                debug_data["results"][key] = result.dict()
            else:
                debug_data["results"][key] = result
    # If results is empty, debug_data["results"] stays as empty dict

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(debug_data, f, indent=2, ensure_ascii=False)

    result_count = len(debug_data["results"])
    print(f"🐛 Saved {tier} debug file: {filename} ({result_count} items)")

def load_debug_results(document_name: str, tier: str):
    """
    Load analysis results from debug file.
    Returns (results_dict, is_valid) tuple.
    """
    filename = get_debug_filename(document_name, tier)

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
            print(f"🔄 Loaded {tier} debug file: {filename} (0 items - empty results)")
            return {}, True

        # Convert back to EnrichedAnalysisResult objects
        results = {}
        for key, result_dict in debug_data["results"].items():
            print(f"\n=== LOADING DEBUG RESULT: {key} ===")
            print(f"Raw taxonomy_relationship_id: {result_dict.get('taxonomy_relationship_id')}")
            
            # Extract taxonomy_relationship_id if present
            taxonomy_relationship_id = result_dict.pop('taxonomy_relationship_id', None)
            print(f"Extracted taxonomy_relationship_id: {taxonomy_relationship_id}")

            # Extract optional hierarchical metadata
            context_data = result_dict.pop('context', {}) or {}
            children_data = result_dict.pop('children', {}) or {}
            
            # Create AnalysisResult from remaining data
            analysis_result = AnalysisResult(**result_dict)
            
            # Create EnrichedAnalysisResult
            enriched_result = EnrichedAnalysisResult(
                analysis_result=analysis_result,
                taxonomy_relationship_id=taxonomy_relationship_id,
                context=context_data,
                children=children_data
            )
            
            print(f"Created EnrichedAnalysisResult:")
            print(f"  - taxonomy_relationship_id: {enriched_result.taxonomy_relationship_id}")
            print(f"  - get('taxonomy_relationship_id'): {enriched_result.get('taxonomy_relationship_id')}")
            print(f"  - hasattr taxonomy_relationship_id: {hasattr(enriched_result, 'taxonomy_relationship_id')}")
            
            results[key] = enriched_result

        print(f"🔄 Loaded {tier} debug file: {filename} ({len(results)} items with relationship IDs)")
        return results, True

    except Exception as e:
        print(f"❌ Error loading debug file {filename}: {e}")
        return None, False


def clean_debug_files(document_name: str, from_tier = None):
    """Clean debug files, optionally from a specific tier onwards."""
    tiers = ["segments", "benefits", "modifiers"]

    if from_tier:
        # Find starting index
        try:
            start_idx = tiers.index(from_tier)
            tiers_to_clean = tiers[start_idx:]
        except ValueError:
            tiers_to_clean = tiers
    else:
        tiers_to_clean = tiers

    for tier in tiers_to_clean:
        filename = get_debug_filename(document_name, tier)
        if os.path.exists(filename):
            os.remove(filename)
            print(f"🗑️  Deleted debug file: {filename}")

class AnalysisResult(BaseModel):
    """Schema for structured extraction of any analysis item (segment, benefit, limit, condition, exclusion) from a document."""
    section_reference: str = Field(description="Reference or identifier for the section.")
    full_text_part: str = Field(description="Full text of the section part.")
    llm_summary: str = Field(description="LLM-generated summary of the section in the language of the input document.")
    item_name: str = Field(description="Name of the item being analyzed in the language of the input document.")
    is_included: bool = Field(description="Indicates if the item is included.")
    description: str = Field(description="Description of what this item covers in the language of the input document.")
    unit: str = Field(description="Unit of measurement if applicable (e.g., CHF, days, percentage).")
    value: float = Field(description="Specific value or amount found in the document.")

@dataclass
class EnrichedAnalysisResult:
    """Analysis result enriched with taxonomy relationship ID and hierarchical context."""
    analysis_result: AnalysisResult
    taxonomy_relationship_id: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    children: Dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name):
        """Delegate attribute access to the underlying AnalysisResult."""
        return getattr(self.analysis_result, name)

    def get(self, key, default=None):
        """Support dict-style .get() access for compatibility with existing code."""
        if key == 'taxonomy_relationship_id':
            return self.taxonomy_relationship_id
        if key == 'context':
            return self.context
        if key == 'children':
            return self.children
        return self.analysis_result.dict().get(key, default)

    def dict(self):
        """Return dictionary representation including taxonomy metadata and hierarchy."""
        result_dict = self.analysis_result.dict()
        result_dict['taxonomy_relationship_id'] = self.taxonomy_relationship_id
        if self.context:
            result_dict['context'] = self.context
        if self.children:
            result_dict['children'] = self.children
        return result_dict


class DocumentAnalyzer:
    """Analyzes insurance documents for segment taxonomy items using GraphQL and OpenAI."""

    def __init__(self,
                 segment_chunk_size: int = 8,
                 benefit_chunk_size: int = 8,
                 modifier_chunk_size: int = 8,
                 debug_mode: bool = False,
                 dcm_id: str = None):
        """
        Initialize DocumentAnalyzer with configurable chunk sizes for each analysis tier.

        Args:
            segment_chunk_size: Number of segments to process in parallel per chunk (default: 8).
            benefit_chunk_size: Number of benefits to process in parallel per chunk (default: 8).
            modifier_chunk_size: Number of modifiers to process in parallel per chunk (default: 3).
                              Smaller default for modifiers due to token-heavy responses.
            debug_mode: Enable debug mode for saving/loading intermediate results.
            dcm_id: Domain Context Model ID for GraphQL taxonomy queries.
        """
        self.segment_chunk_size = segment_chunk_size
        self.benefit_chunk_size = benefit_chunk_size
        self.modifier_chunk_size = modifier_chunk_size
        self.debug_mode = debug_mode
        self.dcm_id = dcm_id
        
        # Initialize debug tracking
        self.product_id = None  # Will be set when analyze_document is called
        self.debug_dir = None   # Will be set up when product_id is available
        self.prompt_response_counter = 0  # Counter for numbering prompt/response files
        
        # Thread-safe execution tracking
        self._execution_lock = asyncio.Lock()
        self._execution_counter = 0

        # Relationship tracking for hierarchical debug data
        self.segment_children_map: Dict[str, List[Dict[str, Any]]] = {}
        self.benefit_context_map: Dict[str, Dict[str, Any]] = {}
        self.benefit_children_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.modifier_context_map: Dict[str, Dict[str, Any]] = {}
        
        self.base_system_prompt = """Sie sind ein hochspezialisierter Experte für die Analyse von schweizerischen Versicherungs-AVB (Allgemeine Versicherungsbedingungen).

KRITISCHE ANALYSEPRINZIPIEN:

1. VOLLSTÄNDIGKEIT UND GRÜNDLICHKEIT:
   - Sie MÜSSEN das gesamte Dokument von der ersten bis zur letzten Seite systematisch durcharbeiten
   - Brechen Sie NIEMALS vorzeitig ab - relevante Informationen können auf der letzten Seite stehen
   - Allgemeine Bestimmungen, Definitionen oder Ausschlüsse am Ende des Dokuments gelten oft für das gesamte Dokument
   - Prüfen Sie sowohl spezifische Abschnitte als auch übergreifende Klauseln

2. ABSOLUTE GENAUIGKEIT:
   - Analysieren Sie AUSSCHLIESSLICH auf Basis der im Dokument explizit vorhandenen Informationen
   - Machen Sie KEINE Annahmen oder Interpretationen über nicht explizit genannte Sachverhalte
   - Wenn eine Information nicht eindeutig im Text steht, behandeln Sie sie als "nicht vorhanden"
   - Verwenden Sie nur die exakten Begriffe und Formulierungen aus dem Originaltext

3. SPRACHVERSTÄNDNIS:
   - Die Dokumente sind in deutscher Sprache verfasst
   - Achten Sie auf schweizerische Rechtsterminologie und spezifische Versicherungsbegriffe
   - Berücksichtigen Sie typische AVB-Strukturen und -Formulierungen
   - Verstehen Sie den Kontext von Versicherungsklauseln und deren rechtliche Bedeutung

4. STRUKTURIERTE DATENEXTRAKTION:
   - Extrahieren Sie alle relevanten Informationen in das vorgegebene JSON-Format
   - Dokumentieren Sie präzise Fundstellen (Abschnittsnummern, Überschriften)
   - Erfassen Sie den vollständigen Wortlaut relevanter Textpassagen
   - Quantifizieren Sie Beträge, Fristen und Limits exakt wie angegeben

5. QUALITÄTSSICHERUNG:
   - Überprüfen Sie Ihre Analyse vor der Rückgabe auf Vollständigkeit
   - Stellen Sie sicher, dass alle Dokumentabschnitte berücksichtigt wurden
   - Validieren Sie, dass alle Angaben direkt aus dem Originaltext stammen
   - Kennzeichnen Sie explizit, wenn bestimmte Informationen nicht auffindbar sind

Die zu analysierenden AVB werden Ihnen als Markdown-formatierter Text in den Benutzernachrichten bereitgestellt.

Ihre Aufgabe ist die präzise, vollständige und dokumentbasierte Analyse der schweizerischen Versicherungs-AVB zur strukturierten Extraktion aller relevanten Versicherungsinformationen."""

        # Get configuration from environment variables
        graphql_url = os.getenv("DIRECTUS_URL", "https://app-uat.quinsights.tech") + "/graphql"
        directus_token = os.getenv("DIRECTUS_AUTH_TOKEN")
        openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        if not directus_token:
            raise ValueError("DIRECTUS_AUTH_TOKEN environment variable is required")

        # Initialize GraphQL client for legacy compatibility (if needed)
        transport = RequestsHTTPTransport(
            url=graphql_url,
            headers={"Authorization": f"Bearer {directus_token}"},
            use_json=True
        )
        self.graphql_client = Client(transport=transport, fetch_schema_from_transport=True)

        # Initialize the unified taxonomy fetcher
        self.taxonomy_fetcher = UnifiedTaxonomyFetcher(graphql_url, directus_token)
        self.taxonomy_data = None

        # Initialize OpenAI model with structured output - NO CACHING
        # Create fresh LLM instance for each call to prevent cache contamination
        self.openai_model = openai_model

        self.segments = []
        self.segment_chains = {}
        
        self.benefits = {}  # Dict[segment_name, List[benefit_dict]]
        self.benefit_chains = {}
        
        # EXTENDED MODIFIER ARCHITECTURE: Three-tier modifier support
        # Product-level modifiers (apply to entire product/policy)
        self.product_modifiers = {  # Dict[modifier_type, List[modifier_dict]]
            'limits': [],
            'conditions': [],
            'exclusions': []
        }
        self.product_modifier_chains = {}
        
        # Segment-level modifiers (apply to specific segments)
        self.segment_modifiers = {  # Dict[segment_name, Dict[modifier_type, List[modifier_dict]]]
            'limits': {},
            'conditions': {},
            'exclusions': {}
        }
        self.segment_modifier_chains = {}
        
        # Benefit-level modifiers (apply to specific benefits) - existing implementation
        self.benefit_modifiers = {  # Dict[benefit_key, Dict[modifier_type, List[modifier_dict]]]
            'limits': {},
            'conditions': {},
            'exclusions': {}
        }
        self.benefit_modifier_chains = {}
        
        # Legacy compatibility (will be deprecated)
        self.modifiers = self.benefit_modifiers  # Backward compatibility
        self.modifier_chains = self.benefit_modifier_chains  # Backward compatibility  # Backward compatibility  # Backward compatibility

    def setup_debug_directory(self, product_id: str):
        """Set up debug directory structure for the product"""
        self.product_id = product_id
        if self.debug_mode:
            self.debug_dir = os.path.join("debug", product_id)
            os.makedirs(self.debug_dir, exist_ok=True)
            print(f"📁 Debug directory created: {self.debug_dir}")

    def _create_fresh_llm(self) -> ChatOpenAI:
        """Create a fresh LLM instance without caching to prevent contamination."""
        return ChatOpenAI(
            model=self.openai_model,
            temperature=0,
            # No caching - each call gets fresh instance
        ).with_structured_output(AnalysisResult)

    async def _get_execution_id(self) -> str:
        """Generate unique execution ID for tracking prompt-response pairs."""
        async with self._execution_lock:
            self._execution_counter += 1
            return f"{self._execution_counter:06d}"
    
    async def log_prompt_and_response(self, execution_id: str, prompt_type: str, item_name: str, 
                                      prompt: str, response: str, context: Dict = None):
        """Thread-safe logging of prompt and response with execution tracking."""
        if not self.debug_mode or not self.debug_dir:
            return
        
        try:
            # Thread-safe counter increment
            async with self._execution_lock:
                self.prompt_response_counter += 1
                counter = self.prompt_response_counter
            
            # Create prompt/response log entry with execution tracking
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "execution_id": execution_id,
                "counter": counter,
                "prompt_type": prompt_type,
                "item_name": item_name,
                "context": context or {},
                "prompt": str(prompt),
                "response": str(response),
                "prompt_hash": hashlib.md5(str(prompt).encode()).hexdigest()[:8],
                "response_hash": hashlib.md5(str(response).encode()).hexdigest()[:8]
            }
            
            # General prompt/response log
            general_log_file = os.path.join(self.debug_dir, "prompts_responses.jsonl")
            with open(general_log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry, ensure_ascii=False, default=str) + "\n")
            
            # Special handling for fixed_copayment - create dedicated file
            if "fixed_copayment" in item_name.lower():
                special_log_file = os.path.join(self.debug_dir, "fixed_copayment_analysis.jsonl")
                with open(special_log_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(log_entry, ensure_ascii=False, default=str) + "\n")
                print(f"🔍 FIXED_COPAYMENT DEBUG: Logged analysis for '{item_name}' to {special_log_file}")
            
            # Create individual files with execution ID for precise tracking
            filename_safe_name = "".join(c for c in item_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            individual_file = os.path.join(self.debug_dir, f"{execution_id}_{counter:03d}_{prompt_type}_{filename_safe_name}.json")
            
            with open(individual_file, "w", encoding="utf-8") as f:
                json.dump({
                    "execution_id": execution_id,
                    "prompt": str(prompt),
                    "response": str(response),
                    "metadata": {
                        "item_name": item_name,
                        "prompt_type": prompt_type,
                        "context": context,
                        "prompt_hash": log_entry["prompt_hash"],
                        "response_hash": log_entry["response_hash"]
                    }
                }, f, ensure_ascii=False, indent=2, default=str)
                
        except Exception as e:
            # Log the error but don't let it break the analysis
            print(f"⚠️ DEBUG LOGGING ERROR: {e}")
            print(f"   Failed to log {prompt_type} for {item_name} (execution_id: {execution_id})")
            # Continue without failing the main analysis
    
    async def execute_llm_with_tracking(self, prompt_type: str, item_info: Dict, 
                                       prompt_template, input_data: Dict) -> AnalysisResult:
        """
        Execute LLM call with thread-safe tracking and no caching contamination.
        Each call gets a fresh LLM instance and unique execution ID.
        """
        execution_id = await self._get_execution_id()
        item_name = item_info.get('name', 'unknown')
        
        try:
            # Format the prompt to see what's actually being sent
            try:
                formatted_prompt = prompt_template.format(**input_data)
            except Exception as e:
                formatted_prompt = f"PROMPT_FORMAT_ERROR: {str(e)}\nTemplate: {str(prompt_template)}\nInputs: {str(input_data)}"
            
            # Create fresh LLM instance to avoid caching contamination
            fresh_llm = self._create_fresh_llm()
            
            # Execute the LLM call directly
            result = await fresh_llm.ainvoke(formatted_prompt)
            
            # Log with execution tracking
            await self.log_prompt_and_response(
                execution_id=execution_id,
                prompt_type=prompt_type,
                item_name=item_name,
                prompt=formatted_prompt,
                response=str(result),
                context={
                    "item_info": item_info,
                    "input_keys": list(input_data.keys()),
                    "fresh_llm_instance": True
                }
            )
            
            return result
            
        except Exception as e:
            error_msg = f"LLM execution failed for {item_name} (execution_id: {execution_id}): {str(e)}"
            print(f"❌ {error_msg}")
            
            # Log the failure
            await self.log_prompt_and_response(
                execution_id=execution_id,
                prompt_type=f"{prompt_type}_ERROR",
                item_name=item_name,
                prompt=formatted_prompt if 'formatted_prompt' in locals() else "PROMPT_UNAVAILABLE",
                response=f"ERROR: {str(e)}",
                context={
                    "item_info": item_info,
                    "error": True,
                    "error_type": type(e).__name__
                }
            )
            
            raise e

    async def execute_parallel_analysis(self, analysis_type: str, items: List[Dict], 
                                       prompt_creator_func, document_text: str, 
                                       chunk_size: int, context_data: Dict = None) -> Dict[str, AnalysisResult]:
        """
        Thread-safe parallel execution with proper prompt-response association.
        
        Args:
            analysis_type: Type of analysis (segment, benefit, modifier)
            items: List of items to analyze
            prompt_creator_func: Function to create prompt for each item
            document_text: Document text for analysis
            chunk_size: Number of items to process in parallel per chunk
            context_data: Additional context for prompt creation
            
        Returns:
            Dict mapping item names to their analysis results
        """
        print(f"🔄 Starting thread-safe parallel {analysis_type} analysis ({len(items)} items, chunk_size: {chunk_size})")
        
        results = {}
        total_items = len(items)
        
        # Process items in chunks for rate limiting
        for chunk_start in range(0, total_items, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_items)
            chunk_items = items[chunk_start:chunk_end]
            chunk_number = (chunk_start // chunk_size) + 1
            total_chunks = (total_items + chunk_size - 1) // chunk_size
            
            print(f"  📦 Processing chunk {chunk_number}/{total_chunks} ({len(chunk_items)} {analysis_type}s)...")
            
            # Create tasks for parallel execution within chunk
            tasks = []
            for item in chunk_items:
                # Create prompt for this specific item
                prompt_template = prompt_creator_func(item, context_data)
                input_data = {"document_text": document_text}

                item_key = item.get('analysis_key') or item.get('name')
                if not item_key:
                    raise ValueError("Analysis items must define either 'analysis_key' or 'name'.")
                
                # Create task with proper item context
                task = self.execute_llm_with_tracking(
                    prompt_type=analysis_type,
                    item_info=item,
                    prompt_template=prompt_template,
                    input_data=input_data
                )
                tasks.append((item_key, task))
            
            # Execute chunk in parallel with proper error handling
            chunk_results = await self._execute_chunk_with_retry(tasks, chunk_number, analysis_type)
            
            # Merge results
            results.update(chunk_results)
            
            print(f"  ✅ Completed chunk {chunk_number}/{total_chunks}")
        
        print(f"🏁 Completed thread-safe parallel {analysis_type} analysis: {len(results)} results")
        return results
    
    @retry(
        stop=stop_after_attempt(6),
        wait=create_smart_wait_strategy(),
        retry=retry_if_exception_type(RateLimitError),
        before_sleep=lambda retry_state: print(f"    Rate limit hit on chunk, retrying... (attempt {retry_state.attempt_number}/6)")
    )
    async def _execute_chunk_with_retry(self, tasks: List[Tuple[str, Coroutine]], 
                                       chunk_number: int, analysis_type: str) -> Dict[str, AnalysisResult]:
        """Execute a chunk of tasks with retry logic and proper association tracking."""
        try:
            # Execute all tasks in parallel
            task_names = [name for name, _ in tasks]
            task_coroutines = [task for _, task in tasks]
            
            print(f"    🚀 Executing {len(tasks)} {analysis_type} tasks in parallel...")
            results_list = await asyncio.gather(*task_coroutines)
            
            # Create results dict with guaranteed correct association
            chunk_results = {}
            for i, (item_name, result) in enumerate(zip(task_names, results_list)):
                chunk_results[item_name] = result
                display_name = self._extract_base_name(item_name)
                print(f"    ✓ {display_name}: {result.item_name} ({'✅ INCLUDED' if result.is_included else '❌ NOT INCLUDED'})")
            
            # Validation: Ensure all results are properly associated
            await self._validate_chunk_results(chunk_results, task_names, analysis_type)
            
            return chunk_results
            
        except Exception as e:
            print(f"    ❌ Error executing {analysis_type} chunk {chunk_number}: {e}")
            raise e

    @staticmethod
    def _extract_base_name(name: str) -> str:
        """Return the base component of a composite analysis key for comparison/logging."""
        if not isinstance(name, str):
            return str(name)
        base_name = name
        for separator in ("::", "|", "__"):
            if separator in base_name:
                base_name = base_name.split(separator)[-1]
        return base_name

    async def _validate_chunk_results(self, results: Dict[str, AnalysisResult], 
                                     expected_names: List[str], analysis_type: str):
        """Validate that results are properly associated with their intended items."""
        validation_errors = []
        
        for expected_name in expected_names:
            if expected_name not in results:
                validation_errors.append(f"Missing result for {expected_name}")
                continue
                
            result = results[expected_name]
            
            # Check if the result item_name matches or is reasonably related
            result_item_name = result.item_name.lower().strip()
            expected_name_clean = expected_name.lower().strip()
            expected_base_name = self._extract_base_name(expected_name_clean)
            
            # Allow some flexibility in name matching but flag major discrepancies
            if result_item_name != expected_base_name:
                # Check if it's a reasonable variation (common in taxonomy names)
                if not (
                    result_item_name in expected_base_name
                    or expected_base_name in result_item_name
                    or result_item_name in expected_name_clean
                    or expected_name_clean in result_item_name
                ):
                    validation_errors.append(
                        f"Potential association mismatch: expected '{expected_name}' but got result for '{result.item_name}'"
                    )
        
        if validation_errors:
            error_msg = f"🚨 VALIDATION ERRORS in {analysis_type} chunk:\n" + "\n".join(validation_errors)
            print(error_msg)
            
            # Log validation errors to debug file
            if self.debug_mode and self.debug_dir:
                validation_log = os.path.join(self.debug_dir, f"validation_errors_{analysis_type}.log")
                with open(validation_log, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()}: {error_msg}\n")
        else:
            print(f"    ✅ Validation passed: All {len(results)} {analysis_type} results properly associated")

    async def fetch_taxonomy_segments(self) -> List[Dict]:
        """Fetch segment taxonomy items and their benefits from GraphQL endpoint using unified fetcher."""
        
        # Use the unified taxonomy fetcher
        self.taxonomy_data = self.taxonomy_fetcher.fetch_taxonomy_data(self.dcm_id)
        
        # Extract data for analysis from the unified fetcher result
        self.segments = self.taxonomy_data.segments
        self.benefits = self.taxonomy_data.benefits
        
        # Map taxonomy details to our new three-tier modifier structure
        # Currently, taxonomy only provides benefit-level modifiers (called "details")
        self.benefit_modifiers = self.taxonomy_data.details  # Benefit-level modifiers from taxonomy
        
        # TODO: In the future, taxonomy should provide product and segment level modifiers
        # For now, these are empty as taxonomy doesn't provide them yet
        # self.product_modifiers = self.taxonomy_data.product_modifiers  # When available
        # self.segment_modifiers = self.taxonomy_data.segment_modifiers  # When available
        
        # Legacy compatibility
        self.modifiers = self.benefit_modifiers
        
        return self.segments

    def create_segment_prompt(self, segment_info: Dict) -> ChatPromptTemplate:
        """Create a prompt template for a specific segment."""

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob das unten beschriebene Versicherungssegment im vorliegenden AVB-Dokument abgedeckt ist. Falls ja, extrahieren Sie alle relevanten Segmentparameter.

**ZU ANALYSIERENDES SEGMENT:**
- **Bezeichnung:** {segment_info['name']}
- **Beschreibung:** {segment_info['description']}
- **Alternative Begriffe:** {segment_info['aliases']}
- **Beispiele:** {segment_info['examples']}

**ANALYSEANWEISUNGEN:**
{segment_info['llm_instruction']}

**ANALYSEKRITERIEN:**
Das Segment ({segment_info['name']}) ist abgedeckt DANN UND NUR DANN (IFF), wenn es explizit im Versicherungsdokument erwähnt oder beschrieben wird.
- Suchen Sie nach direkten Erwähnungen des Segments oder seiner Synonyme
- Prüfen Sie Inhaltsverzeichnisse, Abschnittsüberschriften und Textinhalte
- Berücksichtigen Sie auch indirekte Beschreibungen, die eindeutig auf das Segment hinweisen

**VORGEHEN BEI AUFFINDEN DES SEGMENTS:**
- Extrahieren Sie die relevante Abschnittsreferenz (z.B. Abschnittsnummer, Überschrift)
- Erfassen Sie den vollständigen Wortlaut des relevanten Abschnitts
- Erstellen Sie eine klare Zusammenfassung der Abdeckung
- Setzen Sie is_included auf true

**VORGEHEN WENN SEGMENT NICHT ABGEDECKT:**
- Setzen Sie is_included auf false
- Geben Sie eine kurze Erklärung in der Zusammenfassung
- Verwenden Sie "N/A" für section_reference und full_text_part

**PFLICHTANGABEN:**
- Setzen Sie item_name immer auf: "{segment_info['name']}"
- Setzen Sie description auf: Eine kurze Beschreibung der Segmentabdeckung
- Setzen Sie unit auf: "N/A" (Segmente haben typischerweise keine Einheiten)
- Setzen Sie value auf: 0.0 (Segmente sind Abdeckungsbereiche, keine spezifischen Werte)
            """),
            ("human", "Zu analysierendes AVB-Dokument:\n\n{document_text}")
        ])

        return prompt_template

    def create_benefit_prompt(self, benefit_info: Dict, context_data: Dict = None) -> ChatPromptTemplate:
        """Create a prompt template for a specific benefit with segment context."""
        
        # Extract segment context from benefit_info or context_data
        segment_context = benefit_info.get('segment_context', {})
        segment_name = segment_context.get('segment_name', 'Unknown')
        segment_result = segment_context.get('segment_result')
        
        # Create segment context text
        if segment_result:
            segment_summary = segment_result.llm_summary if hasattr(segment_result, 'llm_summary') else str(segment_result)
            segment_reference = segment_result.section_reference if hasattr(segment_result, 'section_reference') else 'N/A'
        else:
            segment_summary = "Segment wurde identifiziert"
            segment_reference = "N/A"

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob die unten beschriebene Leistung innerhalb des identifizierten Segments im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle relevanten Leistungsparameter.

**SEGMENTKONTEXT:**
Das Segment '{segment_name}' wurde in diesem Dokument identifiziert.
- **Segment-Analysezusammenfassung:** {segment_summary}
- **Fundstelle des Segments:** {segment_reference}

Dieser Kontext hilft Ihnen zu verstehen, dass das Segment existiert, jedoch sollten Sie das GESAMTE Dokument nach der spezifischen Leistung durchsuchen.

**ZU ANALYSIERENDE LEISTUNG:**
- **Bezeichnung:** {benefit_info['name']}
- **Beschreibung:** {benefit_info['description']}
- **Alternative Begriffe:** {benefit_info['aliases']}
- **Beispiele:** {benefit_info['examples']}
- **Einheit:** {benefit_info.get('unit', 'N/A')}
- **Datentyp:** {benefit_info.get('data_type', 'N/A')}

**ANALYSEANWEISUNGEN:**
{benefit_info['llm_instruction']}

**ANALYSEKRITERIEN:**
Die Leistung ({benefit_info['name']}) ist anwendbar DANN UND NUR DANN (IFF), wenn sie in semantischem Zusammenhang mit dem betreffenden Segment ({segment_name}) steht.
- Wenn die Leistung für ein anderes Segment im gesamten Dokument gilt, ist sie HIER, in dieser Instanz, NICHT anwendbar
- Wenn eine Leistung für ein Segment anwendbar ist, steht sie normalerweise in Verbindung mit Abdeckungsmodifikatoren (Bedingungen, Limits und Ausschlüsse), die in einem späteren Stadium extrahiert werden
- Leistungen können überall im Dokument erwähnt werden, nicht nur in dem Abschnitt, wo das Segment identifiziert wurde

**WICHTIGER HINWEIS ZU MODIFIKATOREN:**
Erwähnen Sie in dieser Analyse KEINE spezifischen Modifikatoren (Bedingungen, Limits, Ausschlüsse). Diese werden in einer separaten Analysestufe behandelt. Konzentrieren Sie sich ausschließlich auf die Grundleistung selbst.

**VORGEHEN BEI AUFFINDEN DER LEISTUNG:**
- Extrahieren Sie die relevante Abschnittsreferenz (z.B. Abschnittsnummer, Überschrift)
- Erfassen Sie den vollständigen Wortlaut des relevanten Abschnitts, in dem die Leistung beschrieben wird
- Erstellen Sie eine klare Zusammenfassung der Leistungsabdeckung
- Setzen Sie is_included auf true

**VORGEHEN WENN LEISTUNG NICHT ANWENDBAR:**
- Setzen Sie is_included auf false
- Geben Sie eine kurze Erklärung in der Zusammenfassung
- Verwenden Sie "N/A" für section_reference und full_text_part

**PFLICHTANGABEN:**
- Setzen Sie item_name immer auf: "{benefit_info['name']}"
- Setzen Sie description auf: Eine Beschreibung der spezifischen Leistungsabdeckung
- Setzen Sie unit auf: Die Einheit aus der Taxonomie falls anwendbar: "{benefit_info.get('unit', 'N/A')}"
- Setzen Sie value auf: Den spezifischen Wert/Betrag, der im Dokument für diese Leistung gefunden wurde

**QUALITÄTSSICHERUNG FÜR LEISTUNGSANALYSE:**
1. **VOLLSTÄNDIGKEIT:** Prüfen Sie das gesamte Dokument systematisch nach der Leistung
2. **GENAUIGKEIT:** Verwenden Sie nur explizit im Text vorhandene Informationen
3. **SPRACHVERSTÄNDNIS:** Berücksichtigen Sie schweizerische Versicherungsterminologie
4. **STRUKTURIERTE EXTRAKTION:** Dokumentieren Sie präzise Fundstellen und Wortlaute
5. **VALIDIERUNG:** Bestätigen Sie den semantischen Zusammenhang mit dem Segment
            """),
            ("human", "Zu analysierendes AVB-Dokument:\n\n{document_text}")
        ])

        return prompt_template

    def create_modifier_prompt(self, modifier_info: Dict, context_data: Dict = None) -> ChatPromptTemplate:
        """Create a prompt template for a specific modifier with benefit context."""
        
        modifier_type = modifier_info.get('modifier_type', 'modifier')
        modifier_type_title = modifier_type.upper()
        
        # Extract benefit context
        benefit_context = modifier_info.get('benefit_context', {})
        benefit_name = benefit_context.get('benefit_name', 'Unknown')
        benefit_result = benefit_context.get('benefit_result')
        
        # Create benefit context text
        if benefit_result:
            benefit_summary = benefit_result.llm_summary if hasattr(benefit_result, 'llm_summary') else str(benefit_result)
            benefit_reference = benefit_result.section_reference if hasattr(benefit_result, 'section_reference') else 'N/A'
            benefit_description = benefit_result.description if hasattr(benefit_result, 'description') else 'N/A'
            benefit_value = benefit_result.value if hasattr(benefit_result, 'value') else 'N/A'
            benefit_unit = benefit_result.unit if hasattr(benefit_result, 'unit') else 'N/A'
        else:
            benefit_summary = "Leistung wurde identifiziert"
            benefit_reference = "N/A"
            benefit_description = "N/A"
            benefit_value = "N/A"
            benefit_unit = "N/A"

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob der unten beschriebene Modifikator (Bedingung, Limit oder Ausschluss) innerhalb der identifizierten Leistung im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle erforderlichen Modifikator-Parameter.

**LEISTUNGSKONTEXT:**
Die Leistung '{benefit_name}' wurde in diesem Dokument identifiziert.
- **Leistungs-Analysezusammenfassung:** {benefit_summary}
- **Fundstelle der Leistung:** {benefit_reference}
- **Leistungsbeschreibung:** {benefit_description}
- **Gefundener Leistungswert:** {benefit_value}
- **Leistungseinheit:** {benefit_unit}

Dieser Kontext hilft Ihnen, die Leistung zu verstehen, für die Sie nun die Modifikatoren finden müssen. Der Modifikator ist ein wichtiger Aspekt der aktuellen Leistung. Analysieren Sie das GESAMTE Dokument nach dem spezifischen {modifier_type} unter STÄNDIGER Berücksichtigung der aktuellen Leistung.

**ZU ANALYSIERENDER MODIFIKATOR:**
- **Bezeichnung:** {modifier_info['name']}
- **Beschreibung:** {modifier_info['description']}
- **Alternative Begriffe:** {modifier_info['aliases']}
- **Beispiele:** {modifier_info['examples']}
- **Erwartete Einheit:** {modifier_info.get('unit', 'N/A')}
- **Erwarteter Datentyp:** {modifier_info.get('data_type', 'N/A')}

**ANALYSEANWEISUNGEN FÜR {modifier_type_title}:**
{modifier_info.get('llm_instruction', f'Suchen Sie nach {modifier_type}n im Zusammenhang mit der {benefit_name} Leistung.')}

**ANALYSEKRITERIEN:**
Der Modifikator ({modifier_info['name']}) ist anwendbar DANN UND NUR DANN (IFF), wenn er in semantischem Zusammenhang mit der betreffenden Leistung ({benefit_name}) steht.
- Wenn der Modifikator für eine andere Leistung im gesamten Dokument gilt, ist er HIER, in dieser Instanz, NICHT anwendbar
- Analysieren Sie das gesamte Versicherungsdokument und bestimmen Sie, ob dieser spezifische {modifier_type} erwähnt wird oder auf die {benefit_name} Leistung zutrifft
- {modifier_type_title} können überall im Dokument erwähnt werden, nicht nur dort, wo die Leistung identifiziert wurde

**VORGEHEN BEI AUFFINDEN DES MODIFIKATORS:**
- Extrahieren Sie die relevante Abschnittsreferenz (z.B. Abschnittsnummer, Überschrift)
- Erfassen Sie den vollständigen Wortlaut des relevanten Abschnitts, in dem der {modifier_type} beschrieben wird
- Erstellen Sie eine klare Zusammenfassung dessen, was dieser {modifier_type} spezifiziert
- Setzen Sie is_included auf true
- Setzen Sie description auf: Was dieser {modifier_type} abdeckt oder einschränkt
- Setzen Sie unit auf: Die gefundene Maßeinheit (z.B. CHF, Tage, Prozent) oder "N/A"
- Setzen Sie value auf: Den spezifischen Wert, Betrag oder die Bedingung, die im Dokument gefunden wurde

**VORGEHEN WENN MODIFIKATOR NICHT ERWÄHNT ODER NICHT ZUTREFFEND:**
- Setzen Sie is_included auf false
- Geben Sie eine kurze Erklärung in der Zusammenfassung
- Verwenden Sie "N/A" für section_reference, full_text_part, description und unit
- Verwenden Sie 0.0 für value

**PFLICHTANGABEN:**
- Setzen Sie item_name immer auf: "{modifier_info['name']}"

**QUALITÄTSSICHERUNG FÜR MODIFIKATOR-ANALYSE:**
1. **VOLLSTÄNDIGKEIT:** Prüfen Sie das gesamte Dokument systematisch nach dem Modifikator
2. **GENAUIGKEIT:** Verwenden Sie nur explizit im Text vorhandene Informationen
3. **SPRACHVERSTÄNDNIS:** Berücksichtigen Sie schweizerische Versicherungsterminologie
4. **STRUKTURIERTE EXTRAKTION:** Dokumentieren Sie präzise Fundstellen und Wortlaute
5. **VALIDIERUNG:** Bestätigen Sie den semantischen Zusammenhang mit der Leistung
            """),
            ("human", "Zu analysierendes AVB-Dokument:\n\n{document_text}")
        ])

        return prompt_template

    def create_product_modifier_prompt(self, modifier_info: Dict) -> ChatPromptTemplate:
        """Create a prompt template for a product-level modifier (limit/condition/exclusion) that applies to the entire policy."""

        modifier_type = modifier_info['modifier_type']
        modifier_type_title = modifier_type.upper()

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob der unten beschriebene Modifikator (Bedingung, Limit oder Ausschluss) auf PRODUKTEBENE - d.h. für die GESAMTE Police/das gesamte Versicherungsprodukt - im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle erforderlichen Modifikator-Parameter.

**PRODUKTEBENEN-KONTEXT:**
Sie analysieren einen Modifikator, der sich auf das GESAMTE Versicherungsprodukt/die gesamte Police bezieht, nicht nur auf einzelne Segmente oder Leistungen. Solche Modifikatoren finden sich typischerweise in:
- Allgemeinen Bestimmungen
- Übergreifenden Ausschlüssen
- Produktweiten Limits oder Selbstbehalten  
- Grundsätzlichen Bedingungen für das gesamte Versicherungsprodukt
- Definitionen und allgemeinen Klauseln

**ZU ANALYSIERENDER PRODUKTMODIFIKATOR:**
- **Bezeichnung:** {modifier_info['name']}
- **Beschreibung:** {modifier_info['description']}
- **Alternative Begriffe:** {modifier_info['aliases']}
- **Beispiele:** {modifier_info['examples']}
- **Erwartete Einheit:** {modifier_info.get('unit', 'N/A')}
- **Erwarteter Datentyp:** {modifier_info.get('data_type', 'N/A')}

**ANALYSEANWEISUNGEN FÜR PRODUKT-{modifier_type_title}:**
{modifier_info.get('llm_instruction', f'Suchen Sie nach produktweiten {modifier_type}n, die für das gesamte Versicherungsprodukt gelten.')}

**ANALYSEKRITERIEN:**
Der Produktmodifikator ({modifier_info['name']}) ist anwendbar DANN UND NUR DANN (IFF), wenn er sich auf das GESAMTE Versicherungsprodukt bezieht und nicht nur auf spezifische Segmente oder Leistungen.
- Analysieren Sie das gesamte Versicherungsdokument systematisch von Anfang bis Ende
- Achten Sie besonders auf allgemeine Bestimmungen, Definitionen und übergreifende Klauseln
- Produktweite {modifier_type}s können überall im Dokument erwähnt werden
- Unterscheiden Sie klar zwischen produktweiten und segment-/leistungsspezifischen Modifikatoren

**TYPISCHE FUNDSTELLEN FÜR PRODUKTMODIFIKATOREN:**
- Allgemeine Bestimmungen
- Übergreifende Ausschlüsse
- Grundsätzliche Versicherungsbedingungen
- Produktdefinitionen
- Allgemeine Limits und Selbstbehalte
- Übergreifende zeitliche oder territoriale Einschränkungen

**VORGEHEN BEI AUFFINDEN DES PRODUKTMODIFIKATORS:**
- Extrahieren Sie die relevante Abschnittsreferenz (z.B. Abschnittsnummer, Überschrift)
- Erfassen Sie den vollständigen Wortlaut des relevanten Abschnitts, in dem der {modifier_type} beschrieben wird
- Erstellen Sie eine klare Zusammenfassung dessen, was dieser produktweite {modifier_type} spezifiziert
- Setzen Sie is_included auf true
- Setzen Sie description auf: Was dieser produktweite {modifier_type} abdeckt oder einschränkt
- Setzen Sie unit auf: Die gefundene Maßeinheit (z.B. CHF, Tage, Prozent) oder "N/A"
- Setzen Sie value auf: Den spezifischen Wert, Betrag oder die Bedingung, die im Dokument gefunden wurde

**VORGEHEN WENN PRODUKTMODIFIKATOR NICHT ERWÄHNT ODER NICHT ZUTREFFEND:**
- Setzen Sie is_included auf false
- Geben Sie eine kurze Erklärung in der Zusammenfassung
- Verwenden Sie "N/A" für section_reference, full_text_part, description und unit
- Verwenden Sie 0.0 für value

**PFLICHTANGABEN:**
- Setzen Sie item_name immer auf: "{modifier_info['name']}"

**QUALITÄTSSICHERUNG FÜR PRODUKTMODIFIKATOR-ANALYSE:**
1. **VOLLSTÄNDIGKEIT:** Prüfen Sie das gesamte Dokument systematisch nach dem Produktmodifikator
2. **GENAUIGKEIT:** Verwenden Sie nur explizit im Text vorhandene Informationen
3. **PRODUKTEBENEN-FOKUS:** Stellen Sie sicher, dass der Modifikator wirklich produktweit gilt
4. **SPRACHVERSTÄNDNIS:** Berücksichtigen Sie schweizerische Versicherungsterminologie
5. **STRUKTURIERTE EXTRAKTION:** Dokumentieren Sie präzise Fundstellen und Wortlaute
            """),
            ("human", "Zu analysierendes AVB-Dokument:\\n\\n{document_text}")
        ])

        return prompt_template

    def create_segment_modifier_prompt(self, modifier_info: Dict, segment_result: AnalysisResult) -> ChatPromptTemplate:
        """Create a prompt template for a segment-level modifier (limit/condition/exclusion) with segment context."""

        modifier_type = modifier_info['modifier_type']
        modifier_type_title = modifier_type.upper()

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob der unten beschriebene Modifikator (Bedingung, Limit oder Ausschluss) auf SEGMENTEBENE - d.h. für das gesamte identifizierte Segment, aber nicht für das gesamte Produkt - im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle erforderlichen Modifikator-Parameter.

**SEGMENTKONTEXT:**
Das Segment '{modifier_info['segment_name']}' wurde in diesem Dokument identifiziert.
- **Segment-Analysezusammenfassung:** {segment_result.llm_summary}
- **Fundstelle des Segments:** {segment_result.section_reference}
- **Segmentbeschreibung:** {segment_result.description}
- **Gefundener Segmentwert:** {segment_result.value}
- **Segmenteinheit:** {segment_result.unit}

Sie analysieren einen Modifikator, der sich auf das GESAMTE SEGMENT bezieht, nicht nur auf einzelne Leistungen innerhalb des Segments, aber auch nicht auf das gesamte Versicherungsprodukt. Solche segmentweiten Modifikatoren gelten für ALLE Leistungen innerhalb dieses Segments.

**ZU ANALYSIERENDER SEGMENTMODIFIKATOR:**
- **Bezeichnung:** {modifier_info['name']}
- **Beschreibung:** {modifier_info['description']}
- **Alternative Begriffe:** {modifier_info['aliases']}
- **Beispiele:** {modifier_info['examples']}
- **Erwartete Einheit:** {modifier_info.get('unit', 'N/A')}
- **Erwarteter Datentyp:** {modifier_info.get('data_type', 'N/A')}

**ANALYSEANWEISUNGEN FÜR SEGMENT-{modifier_type_title}:**
{modifier_info.get('llm_instruction', f'Suchen Sie nach {modifier_type}n, die für das gesamte {modifier_info["segment_name"]}-Segment gelten.')}

**ANALYSEKRITERIEN:**
Der Segmentmodifikator ({modifier_info['name']}) ist anwendbar DANN UND NUR DANN (IFF), wenn er sich auf das GESAMTE Segment '{modifier_info['segment_name']}' bezieht, aber nicht produktweit gilt.
- Analysieren Sie das gesamte Versicherungsdokument, wobei Sie sich auf segment-spezifische Bereiche konzentrieren
- Achten Sie auf Modifikatoren, die explizit für dieses Segment gelten, aber nicht für andere Segmente
- Der Modifikator muss sich auf das GESAMTE Segment beziehen, nicht nur auf einzelne Leistungen innerhalb des Segments
- Segmentweite {modifier_type}s können in segment-spezifischen Abschnitten oder in übergreifenden Bestimmungen mit segment-spezifischen Verweisen gefunden werden
- Unterscheiden Sie klar zwischen segmentweiten, leistungsspezifischen und produktweiten Modifikatoren

**TYPISCHE FUNDSTELLEN FÜR SEGMENTMODIFIKATOREN:**
- Segment-spezifische Bestimmungen und Bedingungen
- Übergreifende Ausschlüsse mit segment-spezifischen Verweisen
- Segment-spezifische Limits und Selbstbehalte
- Besondere Bestimmungen für das gesamte Segment
- Segment-spezifische zeitliche oder territoriale Einschränkungen

**VORGEHEN BEI AUFFINDEN DES SEGMENTMODIFIKATORS:**
- Extrahieren Sie die relevante Abschnittsreferenz (z.B. Abschnittsnummer, Überschrift)
- Erfassen Sie den vollständigen Wortlaut des relevanten Abschnitts, in dem der {modifier_type} beschrieben wird
- Erstellen Sie eine klare Zusammenfassung dessen, was dieser segmentweite {modifier_type} spezifiziert
- Setzen Sie is_included auf true
- Setzen Sie description auf: Was dieser segmentweite {modifier_type} für das gesamte Segment abdeckt oder einschränkt
- Setzen Sie unit auf: Die gefundene Maßeinheit (z.B. CHF, Tage, Prozent) oder "N/A"
- Setzen Sie value auf: Den spezifischen Wert, Betrag oder die Bedingung, die im Dokument gefunden wurde

**VORGEHEN WENN SEGMENTMODIFIKATOR NICHT ERWÄHNT ODER NICHT ZUTREFFEND:**
- Setzen Sie is_included auf false
- Geben Sie eine kurze Erklärung in der Zusammenfassung
- Verwenden Sie "N/A" für section_reference, full_text_part, description und unit
- Verwenden Sie 0.0 für value

**PFLICHTANGABEN:**
- Setzen Sie item_name immer auf: "{modifier_info['name']}"

**QUALITÄTSSICHERUNG FÜR SEGMENTMODIFIKATOR-ANALYSE:**
1. **VOLLSTÄNDIGKEIT:** Prüfen Sie das gesamte Dokument systematisch nach dem Segmentmodifikator
2. **GENAUIGKEIT:** Verwenden Sie nur explizit im Text vorhandene Informationen
3. **SEGMENTEBENEN-FOKUS:** Stellen Sie sicher, dass der Modifikator wirklich segmentweit gilt
4. **SPRACHVERSTÄNDNIS:** Berücksichtigen Sie schweizerische Versicherungsterminologie
5. **STRUKTURIERTE EXTRAKTION:** Dokumentieren Sie präzise Fundstellen und Wortlaute
6. **KONTEXTVALIDIERUNG:** Bestätigen Sie den semantischen Zusammenhang mit dem spezifischen Segment
            """),
            ("human", "Zu analysierendes AVB-Dokument:\\n\\n{document_text}")
        ])

        return prompt_template

    def setup_analysis_chains(self):
        """DEPRECATED: Chains are no longer pre-created in the new thread-safe architecture."""
        # No longer needed - chains are created dynamically during execution
        pass

    def setup_benefit_chains(self, segment_results: Dict[str, AnalysisResult]):
        """DEPRECATED: Chains are no longer pre-created in the new thread-safe architecture."""
        # No longer needed - chains are created dynamically during execution
        pass

    async def analyze_benefits(self, document_text: str, found_segments: Dict[str, EnrichedAnalysisResult]) -> Dict[str, EnrichedAnalysisResult]:
        """Execute thread-safe parallel benefit analysis for found segments."""
        print(f"🔄 Starting thread-safe benefit analysis for {len(found_segments)} found segments...")
        
        all_benefits = []
        benefit_context_map: Dict[str, Dict[str, Any]] = {}
        
        # Collect all benefits for found segments
        for segment_name, segment_result in found_segments.items():
            if segment_name in self.benefits:
                segment_benefits = self.benefits[segment_name]
                print(f"  📋 Found {len(segment_benefits)} benefits for segment '{segment_name}'")
                
                # Add segment context to each benefit
                for benefit in segment_benefits:
                    benefit_with_context = benefit.copy()
                    benefit_with_context['segment_context'] = {
                        'segment_name': segment_name,
                        'segment_result': segment_result
                    }
                    analysis_key = f"segment::{segment_name}::{benefit['name']}"
                    benefit_with_context['analysis_key'] = analysis_key
                    benefit_context_map[analysis_key] = {
                        'segment_name': segment_name,
                        'benefit_name': benefit['name'],
                        'segment_included': segment_result.is_included,
                        'segment_taxonomy_relationship_id': getattr(segment_result, 'taxonomy_relationship_id', None)
                    }
                    all_benefits.append(benefit_with_context)
        
        if not all_benefits:
            print("  ❌ No benefits found for analysis")
            return {}
        
        print(f"  📊 Total benefits to analyze: {len(all_benefits)}")
        
        # Create context data for benefit analysis
        context_data = {
            'found_segments': found_segments,
            'document_text': document_text
        }
        
        # Use new thread-safe parallel execution
        raw_results = await self.execute_parallel_analysis(
            analysis_type="benefit",
            items=all_benefits,
            prompt_creator_func=lambda item, context: self.create_benefit_prompt(item, context),
            document_text=document_text,
            chunk_size=self.benefit_chunk_size,
            context_data=context_data
        )
        
        # Enrich results with taxonomy IDs
        taxonomy_items = {}
        for segment_name, segment_benefits in self.benefits.items():
            for benefit in segment_benefits:
                taxonomy_items[benefit['name']] = benefit

        enriched_results = self._enrich_results_with_taxonomy_ids(
            raw_results,
            taxonomy_items,
            context_lookup=benefit_context_map
        )

        # Persist context mapping for downstream tiers
        self.benefit_context_map = {key: value.copy() for key, value in benefit_context_map.items()}

        # Track segment to benefit relationships (preserve latest entry per benefit)
        self.segment_children_map = {}
        for segment_name in found_segments.keys():
            self.segment_children_map[segment_name] = []

        for benefit_key, result in enriched_results.items():
            context = result.context or self.benefit_context_map.get(benefit_key, {})
            segment_name = context.get('segment_name')
            if not segment_name:
                continue

            entry = {
                'benefit_key': benefit_key,
                'taxonomy_relationship_id': result.taxonomy_relationship_id,
                'is_included': result.is_included,
                'item_name': result.item_name
            }

            existing_entries = [child for child in self.segment_children_map.get(segment_name, [])
                                if child.get('benefit_key') != benefit_key]
            existing_entries.append(entry)
            self.segment_children_map[segment_name] = existing_entries
        
        print(f"🏁 Completed thread-safe benefit analysis: {len(enriched_results)} enriched results")
        return enriched_results

    @retry(
        stop=stop_after_attempt(6),
        wait=create_smart_wait_strategy(),
        retry=retry_if_exception_type(RateLimitError),  # Only retry actual rate limit errors
        before_sleep=lambda retry_state: print(f"    Rate limit hit on chunk, retrying... (attempt {retry_state.attempt_number}/6)")
    )
    async def _process_benefit_chunk(self, benefit_runnables: Dict, document_text: str) -> Dict[str, AnalysisResult]:
        """Process a single chunk of benefits with retry logic."""
        # Create RunnableParallel for this chunk
        parallel_analysis = RunnableParallel(benefit_runnables)
        input_data = {"document_text": document_text}

        try:
            # Execute parallel analysis for this chunk
            results = await parallel_analysis.ainvoke(input_data)
            return results
        except Exception as e:
            print(f"    Error analyzing benefit chunk: {e}")
            # Re-raise to trigger retry mechanism only for rate limits
            raise e

    def setup_modifier_chains(self, benefit_results: Dict[str, AnalysisResult], segment_results: Dict[str, AnalysisResult]):
        """Create benefit-level modifier analysis chains for benefits that were found in the document.
        
        NOTE: This method is maintained for backward compatibility and delegates to setup_benefit_modifier_chains.
        """
        self.setup_benefit_modifier_chains(benefit_results, segment_results)

    def setup_benefit_modifier_chains(self, benefit_results: Dict[str, AnalysisResult], segment_results: Dict[str, AnalysisResult]):
        """DEPRECATED: Chains are no longer pre-created in the new thread-safe architecture."""
        # No longer needed - chains are created dynamically during execution
        pass

    def setup_product_modifier_chains(self):
        """DEPRECATED: Chains are no longer pre-created in the new thread-safe architecture."""
        # No longer needed - chains are created dynamically during execution
        pass

    def setup_segment_modifier_chains(self, segment_results: Dict[str, AnalysisResult]):
        """DEPRECATED: Chains are no longer pre-created in the new thread-safe architecture."""
        # No longer needed - chains are created dynamically during execution
        pass

    async def analyze_modifiers(self, document_text: str, found_benefits: Dict[str, EnrichedAnalysisResult]) -> Dict[str, EnrichedAnalysisResult]:
        """Analyze all benefit-level modifiers (limits/conditions/exclusions) for included benefits in chunked parallel processing.
        
        NOTE: This method is maintained for backward compatibility and delegates to analyze_benefit_modifiers.
        """
        return await self.analyze_benefit_modifiers(document_text, found_benefits)

    async def analyze_benefit_modifiers(self, document_text: str, found_benefits: Dict[str, EnrichedAnalysisResult]) -> Dict[str, EnrichedAnalysisResult]:
        """Execute thread-safe parallel analysis of benefit-level modifiers for found benefits."""
        print(f"🔄 Starting thread-safe benefit modifier analysis...")
        
        all_modifiers = []
        modifier_context_map: Dict[str, Dict[str, Any]] = {}
        
        # Collect all modifiers for found benefits
        for benefit_key, benefit_result in found_benefits.items():
            benefit_context = self.benefit_context_map.get(benefit_key, {})
            segment_name = benefit_context.get('segment_name')
            actual_benefit_name = benefit_context.get('benefit_name')

            if not segment_name or not actual_benefit_name:
                print(f"  ⚠️ Warning: Missing benefit context for key '{benefit_key}'")
                continue

            taxonomy_benefit_key = f"{segment_name}_{actual_benefit_name}"
            print(f"  🔍 Checking modifiers for benefit key: '{taxonomy_benefit_key}' (from '{benefit_key}')")
            
            for modifier_type in ['limits', 'conditions', 'exclusions']:
                if taxonomy_benefit_key in self.benefit_modifiers[modifier_type]:
                    benefit_modifiers = self.benefit_modifiers[modifier_type][taxonomy_benefit_key]
                    print(f"  📋 Found {len(benefit_modifiers)} {modifier_type} for benefit '{benefit_key}'")
                    
                    # Add benefit context to each modifier
                    for modifier in benefit_modifiers:
                        modifier_with_context = modifier.copy()
                        modifier_with_context['modifier_type'] = modifier_type
                        modifier_with_context['benefit_context'] = {
                            'benefit_key': benefit_key,
                            'benefit_result': benefit_result,
                            'segment_name': segment_name,
                            'actual_benefit_name': actual_benefit_name
                        }
                        # Create unique tracking identifiers
                        analysis_key = f"modifier::{segment_name}::{actual_benefit_name}::{modifier_type}::{modifier['name']}"
                        modifier_with_context['analysis_key'] = analysis_key
                        modifier_context_map[analysis_key] = {
                            'segment_name': segment_name,
                            'benefit_name': actual_benefit_name,
                            'benefit_key': benefit_key,
                            'modifier_type': modifier_type
                        }
                        # Preserve original modifier name for logging/LLM validation
                        modifier_with_context['name'] = modifier['name']
                        all_modifiers.append(modifier_with_context)
                else:
                    print(f"  📝 No {modifier_type} found for benefit key '{taxonomy_benefit_key}'")
        
        if not all_modifiers:
            print("  ❌ No benefit modifiers found for analysis")
            return {}
        
        print(f"  📊 Total benefit modifiers to analyze: {len(all_modifiers)}")
        
        # Create context data for modifier analysis
        context_data = {
            'found_benefits': found_benefits,
            'document_text': document_text,
            'analysis_level': 'benefit'
        }
        
        # Use new thread-safe parallel execution
        raw_results = await self.execute_parallel_analysis(
            analysis_type="benefit_modifier",
            items=all_modifiers,
            prompt_creator_func=lambda item, context: self.create_modifier_prompt(item, context),
            document_text=document_text,
            chunk_size=self.modifier_chunk_size,
            context_data=context_data
        )
        
        # Enrich results with taxonomy IDs
        taxonomy_items = {modifier['name']: modifier for modifier in all_modifiers}

        enriched_results = self._enrich_results_with_taxonomy_ids(
            raw_results,
            taxonomy_items,
            context_lookup=modifier_context_map
        )

        # Persist modifier context for downstream consumers/debug updates
        self.modifier_context_map = {key: value.copy() for key, value in modifier_context_map.items()}

        # Track benefit to modifier relationships (recomputed per run)
        self.benefit_children_map = {}
        for modifier_key, result in enriched_results.items():
            context = result.context or self.modifier_context_map.get(modifier_key, {})
            parent_benefit_key = context.get('benefit_key')
            modifier_type = context.get('modifier_type', 'modifiers')
            if not parent_benefit_key:
                continue

            entry = {
                'modifier_key': modifier_key,
                'taxonomy_relationship_id': result.taxonomy_relationship_id,
                'is_included': result.is_included,
                'item_name': result.item_name,
                'modifier_type': modifier_type
            }

            benefit_children = self.benefit_children_map.setdefault(parent_benefit_key, {})
            modifiers_for_type = benefit_children.get(modifier_type, [])
            modifiers_for_type = [child for child in modifiers_for_type if child.get('modifier_key') != modifier_key]
            modifiers_for_type.append(entry)
            benefit_children[modifier_type] = modifiers_for_type
            self.benefit_children_map[parent_benefit_key] = benefit_children
        
        print(f"🏁 Completed thread-safe benefit modifier analysis: {len(enriched_results)} enriched results")
        return enriched_results

    async def analyze_product_modifiers(self, document_text: str) -> Dict[str, EnrichedAnalysisResult]:
        """Analyze all product-level modifiers (limits/conditions/exclusions) in chunked parallel processing."""

        if not self.product_modifier_chains:
            return {}

        print(f"Analyzing {len(self.product_modifier_chains)} product-level modifiers in chunks of {self.modifier_chunk_size}...")

        # Convert product_modifier_chains to list for chunking
        modifier_items = list(self.product_modifier_chains.items())
        raw_results = {}

        # Process modifiers in chunks
        for i in range(0, len(modifier_items), self.modifier_chunk_size):
            chunk = modifier_items[i:i + self.modifier_chunk_size]
            chunk_number = (i // self.modifier_chunk_size) + 1
            total_chunks = (len(modifier_items) + self.modifier_chunk_size - 1) // self.modifier_chunk_size
            
            print(f"  Processing product modifier chunk {chunk_number}/{total_chunks} ({len(chunk)} modifiers)...")

            # Create parallel runnables for this chunk
            modifier_runnables = {}
            for modifier_key, modifier_data in chunk:
                modifier_runnables[modifier_key] = modifier_data['chain']
                
            # Process this chunk with retry
            chunk_results = await self._process_modifier_chunk(modifier_runnables, document_text)
            
            # Merge results
            raw_results.update(chunk_results)
            
            print(f"  ✅ Completed product modifier chunk {chunk_number}/{total_chunks}")

        # Enrich results with taxonomy IDs
        # Build taxonomy_items from product modifiers
        taxonomy_items = {}
        for modifier_type in ['limits', 'conditions', 'exclusions']:
            for modifier in self.product_modifiers[modifier_type]:
                modifier_chain_key = f"product_{modifier_type}_{modifier['name']}"
                taxonomy_items[modifier_chain_key] = modifier
                taxonomy_items[modifier['name']] = modifier  # Also allow name-based lookup
        
        return self._enrich_results_with_taxonomy_ids(raw_results, taxonomy_items)

    async def analyze_segment_modifiers(self, document_text: str) -> Dict[str, EnrichedAnalysisResult]:
        """Analyze all segment-level modifiers (limits/conditions/exclusions) in chunked parallel processing."""

        if not self.segment_modifier_chains:
            return {}

        print(f"Analyzing {len(self.segment_modifier_chains)} segment-level modifiers in chunks of {self.modifier_chunk_size}...")

        # Convert segment_modifier_chains to list for chunking
        modifier_items = list(self.segment_modifier_chains.items())
        raw_results = {}

        # Process modifiers in chunks
        for i in range(0, len(modifier_items), self.modifier_chunk_size):
            chunk = modifier_items[i:i + self.modifier_chunk_size]
            chunk_number = (i // self.modifier_chunk_size) + 1
            total_chunks = (len(modifier_items) + self.modifier_chunk_size - 1) // self.modifier_chunk_size
            
            print(f"  Processing segment modifier chunk {chunk_number}/{total_chunks} ({len(chunk)} modifiers)...")

            # Create parallel runnables for this chunk
            modifier_runnables = {}
            for modifier_key, modifier_data in chunk:
                modifier_runnables[modifier_key] = modifier_data['chain']
                
            # Process this chunk with retry
            chunk_results = await self._process_modifier_chunk(modifier_runnables, document_text)
            
            # Merge results
            raw_results.update(chunk_results)
            
            print(f"  ✅ Completed segment modifier chunk {chunk_number}/{total_chunks}")

        # Enrich results with taxonomy IDs
        # Build taxonomy_items from segment modifiers
        taxonomy_items = {}
        for modifier_type in ['limits', 'conditions', 'exclusions']:
            for segment_name, modifiers in self.segment_modifiers[modifier_type].items():
                for modifier in modifiers:
                    modifier_chain_key = f"segment_{segment_name}_{modifier_type}_{modifier['name']}"
                    taxonomy_items[modifier_chain_key] = modifier
                    taxonomy_items[modifier['name']] = modifier  # Also allow name-based lookup
        
        return self._enrich_results_with_taxonomy_ids(raw_results, taxonomy_items)

    @retry(
        stop=stop_after_attempt(6),
        wait=create_smart_wait_strategy(),
        retry=retry_if_exception_type(RateLimitError),  # Only retry actual rate limit errors
        before_sleep=lambda retry_state: print(f"    Rate limit hit on chunk, retrying... (attempt {retry_state.attempt_number}/6)")
    )
    async def _process_modifier_chunk(self, modifier_runnables: Dict, document_text: str) -> Dict[str, AnalysisResult]:
        """Process a single chunk of modifiers with retry logic."""
        # Create RunnableParallel for this chunk
        parallel_analysis = RunnableParallel(modifier_runnables)
        input_data = {"document_text": document_text}

        try:
            # Execute parallel analysis for this chunk
            results = await parallel_analysis.ainvoke(input_data)
            return results
        except Exception as e:
            print(f"    Error analyzing modifier chunk: {e}")
            # Re-raise to trigger retry mechanism only for rate limits
            raise e

    async def _analyze_segments_parallel(self, document_text: str) -> Dict[str, EnrichedAnalysisResult]:
        """Execute thread-safe parallel segment analysis with proper prompt-response association."""
        print(f"🔄 Starting thread-safe segment analysis ({len(self.segments)} segments)...")
        
        # Use new thread-safe parallel execution
        raw_results = await self.execute_parallel_analysis(
            analysis_type="segment",
            items=self.segments,
            prompt_creator_func=lambda item, context: self.create_segment_prompt(item),
            document_text=document_text,
            chunk_size=self.segment_chunk_size,
            context_data=None
        )
        
        # Enrich results with taxonomy IDs
        taxonomy_items = {segment['name']: segment for segment in self.segments}
        enriched_results = self._enrich_results_with_taxonomy_ids(raw_results, taxonomy_items)
        
        print(f"🏁 Completed thread-safe segment analysis: {len(enriched_results)} enriched results")
        return enriched_results

    @retry(
        stop=stop_after_attempt(6),
        wait=create_smart_wait_strategy(),
        retry=retry_if_exception_type(RateLimitError),  # Only retry actual rate limit errors
        before_sleep=lambda retry_state: print(f"    Rate limit hit on chunk, retrying... (attempt {retry_state.attempt_number}/6)")
    )
    async def _process_segment_chunk(self, segment_runnables: Dict, document_text: str) -> Dict[str, AnalysisResult]:
        """Process a single chunk of segments with retry logic."""
        parallel_segment_analysis = RunnableParallel(segment_runnables)
        input_data = {"document_text": document_text}

        try:
            # Execute parallel analysis for this chunk
            results = await parallel_segment_analysis.ainvoke(input_data)
            return results
        except Exception as e:
            print(f"    Error analyzing segment chunk: {e}")
            # Re-raise to trigger retry mechanism only for rate limits
            raise e

    def _enrich_results_with_taxonomy_ids(
        self,
        results: Dict[str, AnalysisResult],
        taxonomy_items: Dict[str, Dict],
        context_lookup: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> Dict[str, EnrichedAnalysisResult]:
        """Enrich AnalysisResult objects with taxonomy_relationship_id from taxonomy data."""
        enriched_results = {}
        
        print(f"\n=== ENRICHMENT DEBUG ===")
        print(f"Enriching {len(results)} analysis results...")
        
        for key, result in results.items():
            item_name = result.item_name
            relationship_id = None
            
            print(f"\nProcessing: key='{key}', item_name='{item_name}'")
            
            # Get the relationship ID directly from properly typed mappings using item_name
            # For this specific DCM product, each item_name should map to exactly one relationship
            if hasattr(self, 'taxonomy_data') and self.taxonomy_data:
                mappings = self.taxonomy_data.mappings
                
                print(f"  Available segment relationships: {list(mappings.segment_relationships.keys())}")
                print(f"  Available benefit relationships: {list(mappings.benefit_relationships.keys())[:3]}...")
                
                # Look up by item_name in the appropriate mapping category
                if item_name in mappings.segment_relationships:
                    relationship_id = mappings.segment_relationships[item_name]
                    print(f"  ✓ Found segment relationship for '{item_name}': {relationship_id[:8]}...")
                
                elif item_name in mappings.benefit_relationships:
                    relationship_id = mappings.benefit_relationships[item_name]
                    print(f"  ✓ Found benefit relationship for '{item_name}': {relationship_id[:8]}...")
                
                elif item_name in mappings.limit_relationships:
                    relationship_id = mappings.limit_relationships[item_name]
                    print(f"  ✓ Found limit relationship for '{item_name}': {relationship_id[:8]}...")
                
                elif item_name in mappings.condition_relationships:
                    relationship_id = mappings.condition_relationships[item_name]
                    print(f"  ✓ Found condition relationship for '{item_name}': {relationship_id[:8]}...")
                
                elif item_name in mappings.exclusion_relationships:
                    relationship_id = mappings.exclusion_relationships[item_name]
                    print(f"  ✓ Found exclusion relationship for '{item_name}': {relationship_id[:8]}...")
                
                else:
                    print(f"  ⚠ No relationship mapping found for item_name '{item_name}'")
            else:
                print(f"  ⚠ No taxonomy_data available")
            
            # Create enriched result
            context_data = context_lookup.get(key, {}).copy() if context_lookup else {}

            enriched_result = EnrichedAnalysisResult(
                analysis_result=result,
                taxonomy_relationship_id=relationship_id,
                context=context_data
            )
            
            print(f"  → Created EnrichedAnalysisResult with taxonomy_relationship_id: {relationship_id}")
            
            # KEEP original key to preserve access patterns
            enriched_results[key] = enriched_result
        
        print(f"=== ENRICHMENT COMPLETE ===\n")
        return enriched_results

    def _attach_relationship_metadata(
        self,
        segment_results: Dict[str, EnrichedAnalysisResult],
        benefit_results: Dict[str, EnrichedAnalysisResult],
        modifier_results: Dict[str, EnrichedAnalysisResult]
    ) -> None:
        """Populate children metadata for segments and benefits using captured context maps."""

        # Build segment -> benefits mapping from current benefit results
        segment_children: Dict[str, List[Dict[str, Any]]] = {}
        for benefit_key, benefit_result in benefit_results.items():
            context = benefit_result.context or self.benefit_context_map.get(benefit_key, {})
            segment_name = context.get('segment_name')
            if not segment_name:
                continue

            entry = {
                'benefit_key': benefit_key,
                'taxonomy_relationship_id': benefit_result.taxonomy_relationship_id,
                'is_included': benefit_result.is_included,
                'item_name': benefit_result.item_name
            }

            children_list = segment_children.get(segment_name, [])
            children_list = [child for child in children_list if child.get('benefit_key') != benefit_key]
            children_list.append(entry)
            segment_children[segment_name] = children_list

        # Update segment results with benefit children metadata
        for segment_name, segment_result in segment_results.items():
            benefit_children = segment_children.get(segment_name, [])
            if benefit_children:
                existing_children = segment_result.children or {}
                existing_children['benefits'] = benefit_children
                segment_result.children = existing_children
            elif segment_result.children.get('benefits'):
                # Ensure stale data removed if no children in current run
                segment_result.children['benefits'] = []

        # Persist for debug updates
        self.segment_children_map = segment_children

        # Build benefit -> modifiers mapping from current modifier results
        benefit_modifiers: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for modifier_key, modifier_result in modifier_results.items():
            context = modifier_result.context or self.modifier_context_map.get(modifier_key, {})
            parent_benefit_key = context.get('benefit_key')
            if not parent_benefit_key:
                continue

            modifier_type = context.get('modifier_type', 'modifiers')
            entry = {
                'modifier_key': modifier_key,
                'taxonomy_relationship_id': modifier_result.taxonomy_relationship_id,
                'is_included': modifier_result.is_included,
                'item_name': modifier_result.item_name,
                'modifier_type': modifier_type
            }

            benefit_entry = benefit_modifiers.setdefault(parent_benefit_key, {})
            modifiers_for_type = benefit_entry.get(modifier_type, [])
            modifiers_for_type = [child for child in modifiers_for_type if child.get('modifier_key') != modifier_key]
            modifiers_for_type.append(entry)
            benefit_entry[modifier_type] = modifiers_for_type
            benefit_modifiers[parent_benefit_key] = benefit_entry

        # Update benefit results with modifier children metadata
        for benefit_key, benefit_result in benefit_results.items():
            modifier_children = benefit_modifiers.get(benefit_key, {})
            if modifier_children:
                existing_children = benefit_result.children or {}
                existing_children['modifiers'] = modifier_children
                benefit_result.children = existing_children
            elif benefit_result.children.get('modifiers'):
                benefit_result.children['modifiers'] = {}

        self.benefit_children_map = benefit_modifiers

    async def analyze_document(self, document_path: str) -> Dict:
        """Analyze a single document for all segments and benefits in parallel with comprehensive three-tier modifier support and debug functionality."""

        # Load the document
        if not os.path.exists(document_path):
            raise FileNotFoundError(f"Document not found: {document_path}")

        with open(document_path, 'r', encoding='utf-8') as f:
            document_text = f.read()

        document_name = Path(document_path).stem
        
        # Set up debug directory using document name as product ID
        self.setup_debug_directory(document_name)

        # Reset relationship tracking for fresh analysis run
        self.segment_children_map = {}
        self.benefit_context_map = {}
        self.benefit_children_map = {}
        self.modifier_context_map = {}

        print(f"=== Analyzing document: {document_name} ===" + (" (Debug Mode)" if self.debug_mode else ""))
        print(f"Document length: {len(document_text)} characters")
        print(f"Using chunk sizes: {self.segment_chunk_size} segments, {self.benefit_chunk_size} benefits, {self.modifier_chunk_size} modifiers per batch")

        # Prepare chunk size metadata for debug files
        chunk_sizes = {
            "segments": self.segment_chunk_size,
            "benefits": self.benefit_chunk_size,
            "modifiers": self.modifier_chunk_size
        }

        try:
            # Step 1: Analyze segments with debug support
            segment_results = None
            if self.debug_mode:
                segment_results, is_valid = load_debug_results(document_name, "segments")

            if segment_results is None:
                print(f"🚀 Running segment analysis ({len(self.segments)} segments)...")
                segment_results = await self._analyze_segments_parallel(document_text)
                
                if self.debug_mode:
                    save_debug_results(document_name, "segments", segment_results, chunk_sizes)

            if segment_results:
                self.segment_children_map = {}
                for segment_name, segment_result in segment_results.items():
                    children = (segment_result.children or {}).get('benefits', []) if segment_result.children else []
                    if children:
                        self.segment_children_map[segment_name] = children

            # Print segment summary
            print(f"\n=== Segment Results Summary for {document_name} ===")
            included_segments = []
            for segment_name, result in segment_results.items():
                status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                print(f"  {segment_name}: {status}")
                if result.is_included:
                    included_segments.append(segment_name)

            # Step 2: Analyze benefits for included segments
            benefit_results = {}
            if included_segments:
                print(f"\nFound {len(included_segments)} segment(s). Proceeding to benefit analysis...")

                # Setup benefit chains for included segments
                self.setup_benefit_chains(segment_results)

                # Analyze benefits with debug support
                if included_segments:  # We have segments, so check for benefits
                    if self.debug_mode:
                        benefit_results, is_valid = load_debug_results(document_name, "benefits")

                    if benefit_results is None or not benefit_results:
                        print(f"🚀 Running benefit analysis for {len(included_segments)} segment(s)...")
                        benefit_results = await self.analyze_benefits(document_text, segment_results)
                        
                        if self.debug_mode:
                            save_debug_results(document_name, "benefits", benefit_results, chunk_sizes)

                    if benefit_results:
                        self.benefit_context_map = {}
                        for benefit_key, benefit_result in benefit_results.items():
                            if benefit_result.context:
                                self.benefit_context_map[benefit_key] = benefit_result.context
                        # Restore any cached modifier relationships from debug if available
                        self.benefit_children_map = {}
                        for benefit_key, benefit_result in benefit_results.items():
                            children = (benefit_result.children or {}).get('modifiers', {}) if benefit_result.children else {}
                            if children:
                                self.benefit_children_map[benefit_key] = children

                    # Print benefit summary
                    print(f"\n=== Benefit Results Summary for {document_name} ===")
                    included_benefits = []
                    for benefit_name, result in benefit_results.items():
                        # In the new architecture, benefit_name is the key and contains segment context
                        status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                        print(f"  {benefit_name}: {status}")
                        if result.is_included:
                            included_benefits.append(benefit_name)

                    # Step 3: THREE-TIER MODIFIER ANALYSIS
                    print(f"\n=== Three-Tier Modifier Analysis for {document_name} ===")

                    # Step 3a: Product-level modifiers
                    print(f"🔄 Step 3a: Product-level modifiers...")
                    self.setup_product_modifier_chains()
                    
                    product_modifier_results = {}
                    if self.product_modifier_chains:
                        if self.debug_mode:
                            product_modifier_results, is_valid = load_debug_results(document_name, "product_modifiers")

                        if product_modifier_results is None or not product_modifier_results:
                            print(f"🚀 Running product modifier analysis ({len(self.product_modifier_chains)} product modifiers)...")
                            product_modifier_results = await self.analyze_product_modifiers(document_text)
                            
                            if self.debug_mode:
                                save_debug_results(document_name, "product_modifiers", product_modifier_results, chunk_sizes)

                        # Print product modifier summary
                        print(f"\n--- Product-Level Modifier Results ---")
                        for modifier_key, result in product_modifier_results.items():
                            modifier_data = self.product_modifier_chains[modifier_key]
                            modifier_type = modifier_data['modifier_type']
                            modifier_name = modifier_data['modifier_info']['name']
                            status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                            print(f"  PRODUCT → {modifier_type}: {modifier_name} {status}")
                    else:
                        print("  No product-level modifiers defined.")

                    # Step 3b: Segment-level modifiers
                    print(f"\n🔄 Step 3b: Segment-level modifiers...")
                    self.setup_segment_modifier_chains(segment_results)
                    
                    segment_modifier_results = {}
                    if self.segment_modifier_chains:
                        if self.debug_mode:
                            segment_modifier_results, is_valid = load_debug_results(document_name, "segment_modifiers")

                        if segment_modifier_results is None or not segment_modifier_results:
                            print(f"🚀 Running segment modifier analysis ({len(self.segment_modifier_chains)} segment modifiers)...")
                            segment_modifier_results = await self.analyze_segment_modifiers(document_text)
                            
                            if self.debug_mode:
                                save_debug_results(document_name, "segment_modifiers", segment_modifier_results, chunk_sizes)

                        # Print segment modifier summary
                        print(f"\n--- Segment-Level Modifier Results ---")
                        for modifier_key, result in segment_modifier_results.items():
                            modifier_data = self.segment_modifier_chains[modifier_key]
                            segment_name = modifier_data['segment_name']
                            modifier_type = modifier_data['modifier_type']
                            modifier_name = modifier_data['modifier_info']['name']
                            status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                            print(f"  {segment_name} → {modifier_type}: {modifier_name} {status}")
                    else:
                        print("  No segment-level modifiers to analyze.")

                    # Step 3c: Benefit-level modifiers (existing functionality)
                    benefit_modifier_results = {}
                    if included_benefits:
                        print(f"\n🔄 Step 3c: Benefit-level modifiers...")
                        print(f"Found {len(included_benefits)} benefit(s). Proceeding to benefit modifier analysis...")

                        # Setup benefit modifier chains for included benefits
                        self.setup_benefit_modifier_chains(benefit_results, segment_results)

                        # Analyze benefit modifiers with debug support
                        if self.benefit_modifier_chains:
                            if self.debug_mode:
                                benefit_modifier_results, is_valid = load_debug_results(document_name, "benefit_modifiers")

                            if benefit_modifier_results is None or not benefit_modifier_results:
                                print(f"🚀 Running benefit modifier analysis ({len(self.benefit_modifier_chains)} benefit modifiers)...")
                                benefit_modifier_results = await self.analyze_benefit_modifiers(document_text, benefit_results)
                                
                            if self.debug_mode:
                                save_debug_results(document_name, "benefit_modifiers", benefit_modifier_results, chunk_sizes)

                        if benefit_modifier_results:
                            self.modifier_context_map = {}
                            for modifier_key, modifier_result in benefit_modifier_results.items():
                                if modifier_result.context:
                                    self.modifier_context_map[modifier_key] = modifier_result.context

                            # Print benefit modifier summary
                            print(f"\n--- Benefit-Level Modifier Results ---")
                            for modifier_name, result in benefit_modifier_results.items():
                                # In new architecture, modifier_name is the key
                                status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                                print(f"  {modifier_name}: {status}")
                        else:
                            print("  No benefit-level modifiers to analyze for the included benefits.")
                    else:
                        print("  No benefits found. Skipping benefit modifier analysis.")

                    # Backward compatibility: Update legacy modifier_results
                    modifier_results = benefit_modifier_results
                else:
                    print("No benefits to analyze for the included segments.")
                    product_modifier_results = {}
                    segment_modifier_results = {}
                    benefit_modifier_results = {}
                    modifier_results = {}
            else:
                print("No segments found. Skipping benefit and modifier analysis.")
                benefit_results = {}
                product_modifier_results = {}
                segment_modifier_results = {}
                benefit_modifier_results = {}
                modifier_results = {}

            # Refresh hierarchical metadata once we have all tiers
            self._attach_relationship_metadata(segment_results, benefit_results, benefit_modifier_results)

            # Update debug artifacts with hierarchical child references if requested
            if self.debug_mode:
                save_debug_results(document_name, "segments", segment_results, chunk_sizes)
                save_debug_results(document_name, "benefits", benefit_results, chunk_sizes)

            # Helper function to create item dictionary with taxonomy_relationship_id
            def create_item_dict(result):
                """Create item dictionary from EnrichedAnalysisResult, preserving taxonomy_relationship_id"""
                item_dict = {
                    "item_name": result.item_name,
                    "is_included": result.is_included,
                    "section_reference": result.section_reference,
                    "full_text_part": result.full_text_part,
                    "llm_summary": result.llm_summary,
                    "description": result.description,
                    "unit": result.unit,
                    "value": result.value
                }
                
                # Add taxonomy_relationship_id if present
                if hasattr(result, 'taxonomy_relationship_id') and result.taxonomy_relationship_id:
                    item_dict["taxonomy_relationship_id"] = result.taxonomy_relationship_id
                elif hasattr(result, 'get') and result.get('taxonomy_relationship_id'):
                    item_dict["taxonomy_relationship_id"] = result.get('taxonomy_relationship_id')
                
                return item_dict

            # Step 4: Build simplified result structure (tree building temporarily disabled)
            # TODO: Reimplement tree structure building for new architecture
            
            simple_results = {
                "segments": [],
                "benefits": {},
                "benefit_modifiers": {}
            }

            for segment_name, segment_result in segment_results.items():
                if not segment_result.is_included:
                    continue

                segment_entry = create_item_dict(segment_result)
                segment_entry.setdefault("benefits", [])
                segment_entry.setdefault("conditions", [])
                segment_entry.setdefault("limits", [])
                segment_entry.setdefault("exclusions", [])

                benefit_children = (segment_result.children or {}).get('benefits', [])

                for child_info in benefit_children:
                    benefit_key = child_info.get('benefit_key')
                    if not benefit_key:
                        continue

                    benefit_result = benefit_results.get(benefit_key)
                    if not benefit_result or not benefit_result.is_included:
                        continue

                    benefit_entry = create_item_dict(benefit_result)
                    benefit_entry.setdefault("conditions", [])
                    benefit_entry.setdefault("limits", [])
                    benefit_entry.setdefault("exclusions", [])

                    modifier_children = (benefit_result.children or {}).get('modifiers', {})
                    for modifier_type in ['conditions', 'limits', 'exclusions']:
                        modifier_entries = []
                        for modifier_child in modifier_children.get(modifier_type, []):
                            modifier_key = modifier_child.get('modifier_key')
                            if not modifier_key:
                                continue

                            modifier_result = benefit_modifier_results.get(modifier_key)
                            if not modifier_result or not modifier_result.is_included:
                                continue

                            modifier_entry = create_item_dict(modifier_result)
                            modifier_entries.append({modifier_key: modifier_entry})
                            simple_results['benefit_modifiers'][modifier_key] = modifier_entry

                        benefit_entry[modifier_type] = modifier_entries

                    segment_entry['benefits'].append({benefit_key: benefit_entry})
                    simple_results['benefits'][benefit_key] = benefit_entry

                simple_results['segments'].append({segment_name: segment_entry})

            return simple_results

        except Exception as e:
            print(f"Error analyzing document {document_name}: {e}")
            raise e

    async def analyze_document_text(self, document_text: str, document_name: str) -> Dict:
        """Analyze document text directly for all segments and benefits in parallel with debug support."""

        # Set up debug directory using document name as product ID
        self.setup_debug_directory(document_name)

        print(f"=== Analyzing document: {document_name} ===" + (" (Debug Mode)" if self.debug_mode else ""))
        print(f"Document length: {len(document_text)} characters")

        # Reset relationship tracking for fresh analysis run
        self.segment_children_map = {}
        self.benefit_context_map = {}
        self.benefit_children_map = {}
        self.modifier_context_map = {}
        print(f"Using chunk sizes: {self.segment_chunk_size} segments, {self.benefit_chunk_size} benefits, {self.modifier_chunk_size} modifiers per batch")

        # Prepare chunk size metadata for debug files
        chunk_sizes = {
            "segments": self.segment_chunk_size,
            "benefits": self.benefit_chunk_size,
            "modifiers": self.modifier_chunk_size
        }

        try:
            # Step 1: Analyze segments with debug support
            segment_results = None
            if self.debug_mode:
                segment_results, is_valid = load_debug_results(document_name, "segments")

            if segment_results is None:
                print(f"🚀 Running segment analysis ({len(self.segments)} segments)...")
                segment_results = await self._analyze_segments_parallel(document_text)
                
                if self.debug_mode:
                    save_debug_results(document_name, "segments", segment_results, chunk_sizes)

            # Print segment summary
            print(f"\n=== Segment Results Summary for {document_name} ===")
            included_segments = []
            for segment_name, result in segment_results.items():
                status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                print(f"  {segment_name}: {status}")
                if result.is_included:
                    included_segments.append(segment_name)

            # Step 2: Analyze benefits for included segments
            benefit_results = {}
            if included_segments:
                print(f"\nFound {len(included_segments)} segment(s). Proceeding to benefit analysis...")

                # Setup benefit chains for included segments
                self.setup_benefit_chains(segment_results)

                # Analyze benefits with debug support
                if included_segments:  # We have segments, so check for benefits
                    if self.debug_mode:
                        benefit_results, is_valid = load_debug_results(document_name, "benefits")

                    if benefit_results is None or not benefit_results:
                        print(f"🚀 Running benefit analysis for {len(included_segments)} segment(s)...")
                        benefit_results = await self.analyze_benefits(document_text, segment_results)
                        
                        if self.debug_mode:
                            save_debug_results(document_name, "benefits", benefit_results, chunk_sizes)

                    # Print benefit summary
                    print(f"\n=== Benefit Results Summary for {document_name} ===")
                    included_benefits = []
                    for benefit_name, result in benefit_results.items():
                        # In the new architecture, benefit_name is the key and contains segment context
                        status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                        print(f"  {benefit_name}: {status}")
                        if result.is_included:
                            included_benefits.append(benefit_name)

                    # Step 3: Analyze benefit modifiers for included benefits
                    modifier_results = {}
                    if included_benefits:
                        print(f"\nFound {len(included_benefits)} benefit(s). Proceeding to benefit modifier analysis...")

                        # Use new thread-safe benefit modifier analysis
                        if self.debug_mode:
                            try:
                                debug_data = load_debug_results(document_name, "benefit_modifiers")
                                if debug_data and isinstance(debug_data, tuple) and len(debug_data) == 2:
                                    modifier_results, is_valid = debug_data
                                    # Ensure modifier_results is a dict
                                    if not isinstance(modifier_results, dict):
                                        print(f"Warning: Debug results not a dict, got {type(modifier_results)}. Using empty dict.")
                                        modifier_results = {}
                                else:
                                    print(f"Warning: Unexpected debug data format: {type(debug_data)}")
                                    modifier_results = {}
                            except Exception as e:
                                print(f"Error loading debug results: {e}")
                                modifier_results = {}

                        if not modifier_results:
                            print(f"🚀 Running benefit modifier analysis...")
                            modifier_results = await self.analyze_benefit_modifiers(document_text, benefit_results)
                            
                            # Ensure modifier_results is a dict before saving
                            if not isinstance(modifier_results, dict):
                                print(f"Warning: analyze_benefit_modifiers returned {type(modifier_results)}, expected dict")
                                modifier_results = {}
                            
                            if self.debug_mode:
                                save_debug_results(document_name, "benefit_modifiers", modifier_results, chunk_sizes)

                        # Print modifier summary
                        print(f"\n=== Benefit Modifier Results Summary for {document_name} ===")
                        # Ensure modifier_results is a dictionary before processing
                        if isinstance(modifier_results, dict):
                            if modifier_results:
                                for modifier_name, result in modifier_results.items():
                                    status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                                    print(f"  {modifier_name}: {status}")
                            else:
                                print("  No modifiers found.")
                        else:
                            print(f"  Error: modifier_results is not a dict, got: {type(modifier_results)}. Using empty dict.")
                            modifier_results = {}
                    else:
                        print("\n  No benefits found for modifier analysis.")
                        modifier_results = {}
                else:
                    print("No benefits to analyze for the included segments.")
                    modifier_results = {}
            else:
                print("No segments found. Skipping benefit and modifier analysis.")
                benefit_results = {}
                modifier_results = {}

            # Step 4: Export results if requested
            simple_results = {
                "segments": {},
                "benefits": {},
                "benefit_modifiers": {}
            }
            
            # Helper function to create item dictionary with taxonomy_relationship_id
            def create_item_dict(result):
                """Create item dictionary from EnrichedAnalysisResult, preserving taxonomy_relationship_id"""
                item_dict = {
                    "item_name": result.item_name,
                    "is_included": result.is_included,
                    "section_reference": result.section_reference,
                    "full_text_part": result.full_text_part,
                    "llm_summary": result.llm_summary,
                    "description": result.description,
                    "unit": result.unit,
                    "value": result.value
                }
                
                # Add taxonomy_relationship_id if present
                if hasattr(result, 'taxonomy_relationship_id') and result.taxonomy_relationship_id:
                    item_dict["taxonomy_relationship_id"] = result.taxonomy_relationship_id
                elif hasattr(result, 'get') and result.get('taxonomy_relationship_id'):
                    item_dict["taxonomy_relationship_id"] = result.get('taxonomy_relationship_id')
                
                return item_dict
            
            # Add segment results
            for segment_name, result in segment_results.items():
                if result.is_included:
                    simple_results["segments"][segment_name] = create_item_dict(result)
            
            # Add benefit results  
            for benefit_name, result in benefit_results.items():
                if result.is_included:
                    simple_results["benefits"][benefit_name] = create_item_dict(result)
            
            # Add benefit modifier results
            if isinstance(modifier_results, dict):
                for modifier_name, result in modifier_results.items():
                    if result.is_included:
                        simple_results["benefit_modifiers"][modifier_name] = create_item_dict(result)

            return simple_results

        except Exception as e:
            print(f"Error analyzing document {document_name}: {e}")
            raise e

    def export_results(self, results: Dict, document_path: str):
        """Export results to JSON file."""

        document_name = Path(document_path).stem
        filename = f"{document_name}_analysis_results.json"

        # Convert Pydantic models to dictionaries in the new hierarchical structure
        serializable_results = convert_pydantic_to_dict(results)

        with open('exports/' + filename, 'w', encoding='utf-8') as f:
            json.dump({document_name: serializable_results}, f, indent=2, ensure_ascii=False)

        print(f"Results exported to {filename}")

    def print_detailed_results(self, results: Dict, document_path: str):
        """Print detailed results for the document."""

        document_name = Path(document_path).stem

        print(f"\n{'='*60}")
        print(f"DETAILED RESULTS: {document_name.upper()}")
        print(f"{'='*60}")

        # Print segment details from new hierarchical structure
        segments = results.get('segments', [])
        if segments:
            print(f"\n{'='*40}")
            print("SEGMENTS")
            print(f"{'='*40}")

            for segment_item in segments:
                for segment_name, segment_data in segment_item.items():
                    print(f"\n--- SEGMENT: {segment_name.upper()} ---")
                    print(f"Included: {'✓ YES' if segment_data.get('is_included', False) else '✗ NO'}")
                    print(f"Section Reference: {segment_data.get('section_reference', 'N/A')}")
                    print(f"Description: {segment_data.get('description', 'N/A')}")
                    print(f"Summary: {segment_data.get('llm_summary', 'N/A')}")
                    print(f"Unit: {segment_data.get('unit', 'N/A')}")
                    print(f"Value: {segment_data.get('value', '0.0')}")
                    
                    full_text = segment_data.get('full_text_part', 'N/A')
                    if segment_data.get('is_included', False) and full_text != "N/A":
                        print(f"Full Text (first 300 chars): {full_text[:300]}...")

                    # Print benefits for this segment
                    benefits = segment_data.get('benefits', [])
                    if benefits:
                        print(f"\n  BENEFITS FOR {segment_name.upper()}:")
                        for benefit_item in benefits:
                            for benefit_name, benefit_data in benefit_item.items():
                                print(f"\n  --- BENEFIT: {benefit_name.upper()} ---")
                                print(f"  Included: {'✓ YES' if benefit_data.get('is_included', False) else '✗ NO'}")
                                print(f"  Section Reference: {benefit_data.get('section_reference', 'N/A')}")
                                print(f"  Description: {benefit_data.get('description', 'N/A')}")
                                print(f"  Summary: {benefit_data.get('llm_summary', 'N/A')}")
                                print(f"  Unit: {benefit_data.get('unit', 'N/A')}")
                                print(f"  Value: {benefit_data.get('value', 0.0)}")

                                full_text = benefit_data.get('full_text_part', 'N/A')
                                if benefit_data.get('is_included', False) and full_text != "N/A":
                                    print(f"  Full Text (first 300 chars): {full_text[:300]}...")

                                # Print modifiers for this benefit
                                for modifier_type in ['limits', 'conditions', 'exclusions']:
                                    modifiers = benefit_data.get(modifier_type, [])
                                    if modifiers:
                                        print(f"\n    {modifier_type.upper()} FOR {benefit_name.upper()}:")
                                        for modifier_item in modifiers:
                                            for modifier_name, modifier_data in modifier_item.items():
                                                print(f"\n    --- {modifier_type.upper()}: {modifier_name.upper()} ---")
                                                print(f"    Included: {'✓ YES' if modifier_data.get('is_included', False) else '✗ NO'}")
                                                print(f"    Section Reference: {modifier_data.get('section_reference', 'N/A')}")
                                                print(f"    Description: {modifier_data.get('description', 'N/A')}")
                                                print(f"    Summary: {modifier_data.get('llm_summary', 'N/A')}")
                                                print(f"    Unit: {modifier_data.get('unit', 'N/A')}")
                                                print(f"    Value: {modifier_data.get('value', 0.0)}")

                                                full_text = modifier_data.get('full_text_part', 'N/A')
                                                if modifier_data.get('is_included', False) and full_text != "N/A":
                                                    print(f"    Full Text (first 200 chars): {full_text[:200]}...")
        else:
            print(f"\nNo segments were found in the document.")
