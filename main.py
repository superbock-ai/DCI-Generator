import asyncio
import argparse
import json
import os
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel
from langchain_core.globals import set_llm_cache
from langchain_core.caches import InMemoryCache
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
from openai import RateLimitError
import re
import math

# Import Directus seeding functionality
from directus_seeder import seed_to_directus, cleanup_seeded_data

# Load environment variables from .env file
load_dotenv()

# Set up in-memory caching for LangChain
set_llm_cache(InMemoryCache())


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
    return f"{document_name}_{tier}.debug.json"


def save_debug_results(document_name: str, tier: str, results: dict, chunk_sizes: dict):
    """Save analysis results to debug file with chunk size metadata."""
    filename = get_debug_filename(document_name, tier)

    debug_data = {
        "tier": tier,
        "chunk_sizes": chunk_sizes,
        "results": {}
    }

    # Convert Pydantic models to dictionaries
    for key, result in results.items():
        if hasattr(result, 'dict'):  # Pydantic model
            debug_data["results"][key] = result.dict()
        else:
            debug_data["results"][key] = result

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(debug_data, f, indent=2, ensure_ascii=False)

    print(f"🐛 Saved {tier} debug file: {filename}")


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

        # Convert back to AnalysisResult objects
        results = {}
        for key, result_dict in debug_data["results"].items():
            results[key] = AnalysisResult(**result_dict)

        print(f"🔄 Loaded {tier} debug file: {filename} ({len(results)} items)")
        return results, True

    except Exception as e:
        print(f"❌ Error loading debug file {filename}: {e}")
        return None, False


def clean_debug_files(document_name: str, from_tier = None):
    """Clean debug files, optionally from a specific tier onwards."""
    tiers = ["segments", "benefits", "details"]

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
    value: str = Field(description="Specific value or amount found in the document.")


class DocumentAnalyzer:
    """Analyzes insurance documents for segment taxonomy items using GraphQL and OpenAI."""

    def __init__(self,
                 segment_chunk_size: int = 8,
                 benefit_chunk_size: int = 8,
                 detail_chunk_size: int = 3,
                 debug_mode: bool = False):
        """
        Initialize DocumentAnalyzer with configurable chunk sizes for each analysis tier.

        Args:
            segment_chunk_size: Number of segments to process in parallel per chunk (default: 8).
            benefit_chunk_size: Number of benefits to process in parallel per chunk (default: 8).
            detail_chunk_size: Number of details to process in parallel per chunk (default: 3).
                              Smaller default for details due to token-heavy responses.
            debug_mode: Enable debug mode for saving/loading intermediate results.
        """
        self.segment_chunk_size = segment_chunk_size
        self.benefit_chunk_size = benefit_chunk_size
        self.detail_chunk_size = detail_chunk_size
        self.debug_mode = debug_mode
        
        self.base_system_prompt = """You are an expert for analysing insurance documents.
                        Your job is to extract relevant information from the document in a structured json format.
                        You will receive a markdown version of the insurance document and analyse the document for the following information:"""

        # Get configuration from environment variables
        graphql_url = os.getenv("GRAPHQL_URL", "https://app-uat.quinsights.tech/graphql")
        graphql_token = os.getenv("GRAPHQL_AUTH_TOKEN")
        openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        if not graphql_token:
            raise ValueError("GRAPHQL_AUTH_TOKEN environment variable is required")

        # Initialize GraphQL client
        transport = RequestsHTTPTransport(
            url=graphql_url,
            headers={"Authorization": f"Bearer {graphql_token}"},
            use_json=True
        )
        self.graphql_client = Client(transport=transport, fetch_schema_from_transport=True)

        # Initialize OpenAI model with structured output
        # Retry logic handled by decorators on the methods that make API calls
        self.llm = ChatOpenAI(
            model=openai_model,
            temperature=0,
        ).with_structured_output(AnalysisResult)

        self.segments = []
        self.segment_chains = {}
        self.benefits = {}  # Dict[segment_name, List[benefit_dict]]
        self.benefit_chains = {}
        self.details = {  # Dict[benefit_key, Dict[detail_type, List[detail_dict]]
            'limits': {},
            'conditions': {},
            'exclusions': {}
        }
        self.detail_chains = {}

    async def fetch_taxonomy_segments(self) -> List[Dict]:
        """Fetch segment taxonomy items and their benefits from GraphQL endpoint."""

        # Read the GraphQL query
        with open("graphql/GetCompleteTaxonomyHierarchy.graphql") as f:
            query = f.read()

        # Variables for the query
        variables = {"dcm_id": "d427fe94-fc61-4269-8584-78556a36758c"}

        # Execute the query
        result = self.graphql_client.execute(gql(query), variable_values=variables)

        # Extract segment_type items and their benefits from the response
        segments = []
        for item in result['taxonomy_items']:
            if item['category'] == 'product_type':
                for parent_rel in item['parent_relationships']:
                    segment_item = parent_rel['related_taxonomy_item']
                    if segment_item['category'] == 'segment_type':
                        segment_name = segment_item['taxonomy_item_name']

                        # Extract segment info
                        segment_data = {
                            'id': segment_item['id'],
                            'name': segment_name,
                            'description': segment_item['description'],
                            'aliases': segment_item['aliases'],
                            'examples': segment_item['examples'],
                            'llm_instruction': segment_item['llm_instruction']
                        }
                        segments.append(segment_data)

                        # Extract benefits for this segment
                        benefits = []
                        for benefit_rel in segment_item.get('parent_relationships', []):
                            benefit_item = benefit_rel['related_taxonomy_item']
                            if benefit_item['category'] == 'benefit_type':
                                benefit_data = {
                                    'id': benefit_item['id'],
                                    'name': benefit_item['taxonomy_item_name'],
                                    'description': benefit_item['description'],
                                    'aliases': benefit_item['aliases'],
                                    'examples': benefit_item['examples'],
                                    'llm_instruction': benefit_item['llm_instruction'],
                                    'segment_name': segment_name,
                                    'unit': benefit_item.get('unit'),
                                    'data_type': benefit_item.get('data_type')
                                }
                                benefits.append(benefit_data)

                                # Extract limits, conditions, and exclusions for this benefit
                                benefit_key = f"{segment_name}_{benefit_item['taxonomy_item_name']}"

                                # Extract limits
                                limits = []
                                for limit_rel in benefit_item.get('benefit_limits', []):
                                    limit_item = limit_rel['related_taxonomy_item']
                                    limits.append({
                                        'id': limit_item['id'],
                                        'name': limit_item['taxonomy_item_name'],
                                        'description': limit_item['description'],
                                        'aliases': limit_item['aliases'],
                                        'examples': limit_item['examples'],
                                        'llm_instruction': limit_item.get('llm_instruction', ''),
                                        'unit': limit_item.get('unit'),
                                        'data_type': limit_item.get('data_type'),
                                        'benefit_name': benefit_item['taxonomy_item_name'],
                                        'segment_name': segment_name,
                                        'detail_type': 'limit'
                                    })

                                # Extract conditions
                                conditions = []
                                for condition_rel in benefit_item.get('benefit_conditions', []):
                                    condition_item = condition_rel['related_taxonomy_item']
                                    conditions.append({
                                        'id': condition_item['id'],
                                        'name': condition_item['taxonomy_item_name'],
                                        'description': condition_item['description'],
                                        'aliases': condition_item['aliases'],
                                        'examples': condition_item['examples'],
                                        'llm_instruction': condition_item.get('llm_instruction', ''),
                                        'unit': condition_item.get('unit'),
                                        'data_type': condition_item.get('data_type'),
                                        'benefit_name': benefit_item['taxonomy_item_name'],
                                        'segment_name': segment_name,
                                        'detail_type': 'condition'
                                    })

                                # Extract exclusions
                                exclusions = []
                                for exclusion_rel in benefit_item.get('benefit_exclusions', []):
                                    exclusion_item = exclusion_rel['related_taxonomy_item']
                                    exclusions.append({
                                        'id': exclusion_item['id'],
                                        'name': exclusion_item['taxonomy_item_name'],
                                        'description': exclusion_item['description'],
                                        'aliases': exclusion_item['aliases'],
                                        'examples': exclusion_item['examples'],
                                        'llm_instruction': exclusion_item.get('llm_instruction', ''),
                                        'unit': exclusion_item.get('unit'),
                                        'data_type': exclusion_item.get('data_type'),
                                        'benefit_name': benefit_item['taxonomy_item_name'],
                                        'segment_name': segment_name,
                                        'detail_type': 'exclusion'
                                    })

                                # Store details organized by benefit
                                self.details['limits'][benefit_key] = limits
                                self.details['conditions'][benefit_key] = conditions
                                self.details['exclusions'][benefit_key] = exclusions

                        self.benefits[segment_name] = benefits

        return segments

    def create_segment_prompt(self, segment_info: Dict) -> ChatPromptTemplate:
        """Create a prompt template for a specific segment."""

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

    Specific Instructions for this segment:
    {segment_info['llm_instruction']}

    You are analyzing the document for the presence of the segment: "{segment_info['name']}"
    Description: {segment_info['description']}
    Aliases: {segment_info['aliases']}
    Examples: {segment_info['examples']}

    Analyze the provided insurance document and determine if this segment is covered. If you find coverage for this segment:
    - Extract the relevant section reference (e.g., section number, heading)
    - Include the full text of the relevant section
    - Provide a clear summary of what is covered
    - Set is_included to true

    If the segment is not covered:
    - Set is_included to false
    - Provide a brief explanation in the summary
    - Use "N/A" for section_reference and full_text_part

    Always set item_name to: "{segment_info['name']}"
Set description to: A brief description of what this segment covers
Set unit to: "N/A" (segments typically don't have units)
Set value to: "N/A" (segments are coverage areas, not specific values)
    """),
            ("human", "Document to analyze:\n\n{document_text}")
        ])

        return prompt_template

    def create_benefit_prompt(self, benefit_info: Dict, segment_result: AnalysisResult) -> ChatPromptTemplate:
        """Create a prompt template for a specific benefit with segment context."""

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

SEGMENT CONTEXT:
The segment '{benefit_info['segment_name']}' was identified in this document.
Segment analysis summary: {segment_result.llm_summary}
Section where segment was found: {segment_result.section_reference}

This context helps you understand that the segment exists, but you should analyze the ENTIRE document for the specific benefit described below.

BENEFIT ANALYSIS INSTRUCTIONS:
{benefit_info['llm_instruction']}

You are analyzing the entire document for the presence of the benefit: "{benefit_info['name']}"
Description: {benefit_info['description']}
Aliases: {benefit_info['aliases']}
Examples: {benefit_info['examples']}
Unit: {benefit_info.get('unit', 'N/A')}
Data Type: {benefit_info.get('data_type', 'N/A')}

Analyze the entire insurance document and determine if this specific benefit is covered. Benefits may be mentioned anywhere in the document, not just in the section where the segment was identified.

If you find coverage for this benefit:
- Extract the relevant section reference (e.g., section number, heading)
- Include the full text of the relevant section where the benefit is described
- Provide a clear summary of what is covered for this benefit
- Set is_included to true

If the benefit is not covered:
- Set is_included to false
- Provide a brief explanation in the summary
- Use "N/A" for section_reference and full_text_part

Always set item_name to: "{benefit_info['name']}"
Set description to: A description of what this specific benefit covers
Set unit to: The unit from the taxonomy if applicable: "{benefit_info.get('unit', 'N/A')}"
Set value to: The specific value/amount found in the document for this benefit
"""),
            ("human", "Document to analyze:\n\n{document_text}")
        ])

        return prompt_template

    def create_detail_prompt(self, detail_info: Dict, segment_result: AnalysisResult, benefit_result: AnalysisResult) -> ChatPromptTemplate:
        """Create a prompt template for a specific detail (limit/condition/exclusion) with segment and benefit context."""

        detail_type = detail_info['detail_type']
        detail_type_title = detail_type.upper()

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", f"""{self.base_system_prompt}

SEGMENT CONTEXT:
The segment '{detail_info['segment_name']}' was identified in this document.
Segment analysis summary: {segment_result.llm_summary}
Section where segment was found: {segment_result.section_reference}

BENEFIT CONTEXT:
The benefit '{detail_info['benefit_name']}' was identified within this segment.
Benefit analysis summary: {benefit_result.llm_summary}
Section where benefit was found: {benefit_result.section_reference}
Benefit coverage description: {benefit_result.description}
Benefit value found: {benefit_result.value}
Benefit unit: {benefit_result.unit}

This context helps you understand the segment and benefit for which you now need to find the details for. The detail is an important aspect of the current benefit. Analyze the ENTIRE document for the specific {detail_type} described below ALWAYS having the current segment and benefit in mind.

{detail_type_title} ANALYSIS INSTRUCTIONS:
{detail_info.get('llm_instruction', f'Look for {detail_type}s related to the {detail_info["benefit_name"]} benefit.')}

You are analyzing the entire document for the presence of the {detail_type}: "{detail_info['name']}"
Description: {detail_info['description']}
Aliases: {detail_info['aliases']}
Examples: {detail_info['examples']}
Expected Unit: {detail_info.get('unit', 'N/A')}
Expected Data Type: {detail_info.get('data_type', 'N/A')}

Analyze the entire insurance document and determine if this specific {detail_type} is mentioned or applies to the {detail_info['benefit_name']} benefit. {detail_type_title}s may be mentioned anywhere in the document, not just where the benefit was identified.

If you find this {detail_type}:
- Extract the relevant section reference (e.g., section number, heading)
- Include the full text of the relevant section where the {detail_type} is described
- Provide a clear summary of what this {detail_type} specifies
- Set is_included to true
- Set description to: What this {detail_type} covers or restricts
- Set unit to: The unit of measurement found (e.g., CHF, days, percentage) or "N/A"
- Set value to: The specific value, amount, or condition found in the document

If the {detail_type} is not mentioned or doesn't apply:
- Set is_included to false
- Provide a brief explanation in the summary
- Use "N/A" for section_reference, full_text_part, description, unit, and value

Always set item_name to: "{detail_info['name']}"
"""),
            ("human", "Document to analyze:\n\n{document_text}")
        ])

        return prompt_template

    def setup_analysis_chains(self):
        """Create analysis chains for all segments."""

        for segment in self.segments:
            prompt = self.create_segment_prompt(segment)
            chain = prompt | self.llm
            self.segment_chains[segment['name']] = chain

    def setup_benefit_chains(self, segment_results: Dict[str, AnalysisResult]):
        """Create benefit analysis chains for segments that were found in the document."""

        self.benefit_chains = {}

        # Only create benefit chains for segments that exist in the document
        for segment_name, segment_result in segment_results.items():
            if segment_result.is_included and segment_name in self.benefits:
                # Create chains for all benefits of this included segment
                for benefit in self.benefits[segment_name]:
                    benefit_key = f"{segment_name}_{benefit['name']}"
                    prompt = self.create_benefit_prompt(benefit, segment_result)
                    chain = prompt | self.llm
                    self.benefit_chains[benefit_key] = {
                        'chain': chain,
                        'benefit_info': benefit,
                        'segment_name': segment_name
                    }

    async def analyze_benefits(self, document_text: str) -> Dict[str, AnalysisResult]:
        """Analyze all benefits for included segments in chunked parallel processing."""

        if not self.benefit_chains:
            return {}

        print(f"Analyzing {len(self.benefit_chains)} benefits in chunks of {self.benefit_chunk_size}...")

        # Convert benefit_chains to list for chunking
        benefit_items = list(self.benefit_chains.items())
        all_results = {}

        # Process benefits in chunks
        for i in range(0, len(benefit_items), self.benefit_chunk_size):
            chunk = benefit_items[i:i + self.benefit_chunk_size]
            chunk_number = (i // self.benefit_chunk_size) + 1
            total_chunks = (len(benefit_items) + self.benefit_chunk_size - 1) // self.benefit_chunk_size
            
            print(f"  Processing chunk {chunk_number}/{total_chunks} ({len(chunk)} benefits)...")

            # Create parallel runnables for this chunk
            benefit_runnables = {}
            for benefit_key, benefit_data in chunk:
                benefit_runnables[benefit_key] = benefit_data['chain']

            # Process this chunk with retry
            chunk_results = await self._process_benefit_chunk(benefit_runnables, document_text)
            
            # Merge results
            all_results.update(chunk_results)
            
            print(f"  ✅ Completed chunk {chunk_number}/{total_chunks}")

        return all_results

    @retry(
        stop=stop_after_attempt(6),
        wait=create_smart_wait_strategy(),
        retry=retry_if_exception_type((RateLimitError, Exception)),
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
            # Re-raise to trigger retry mechanism
            raise e

    def setup_detail_chains(self, benefit_results: Dict[str, AnalysisResult], segment_results: Dict[str, AnalysisResult]):
        """Create detail analysis chains for benefits that were found in the document."""

        self.detail_chains = {}

        # Only create detail chains for benefits that exist in the document
        for benefit_key, benefit_result in benefit_results.items():
            if benefit_result.is_included:
                # Get corresponding segment result
                segment_name = self.benefit_chains[benefit_key]['segment_name']
                segment_result = segment_results[segment_name]

                # Create chains for all details (limits, conditions, exclusions) of this benefit
                for detail_type in ['limits', 'conditions', 'exclusions']:
                    details = self.details[detail_type].get(benefit_key, [])
                    for detail in details:
                        detail_chain_key = f"{benefit_key}_{detail_type}_{detail['name']}"
                        prompt = self.create_detail_prompt(detail, segment_result, benefit_result)
                        chain = prompt | self.llm
                        self.detail_chains[detail_chain_key] = {
                            'chain': chain,
                            'detail_info': detail,
                            'detail_type': detail_type,
                            'benefit_key': benefit_key,
                            'segment_name': segment_name
                        }

    async def analyze_details(self, document_text: str) -> Dict[str, AnalysisResult]:
        """Analyze all details (limits/conditions/exclusions) for included benefits in chunked parallel processing."""

        if not self.detail_chains:
            return {}

        print(f"Analyzing {len(self.detail_chains)} details in chunks of {self.detail_chunk_size}...")

        # Convert detail_chains to list for chunking
        detail_items = list(self.detail_chains.items())
        all_results = {}

        # Process details in chunks
        for i in range(0, len(detail_items), self.detail_chunk_size):
            chunk = detail_items[i:i + self.detail_chunk_size]
            chunk_number = (i // self.detail_chunk_size) + 1
            total_chunks = (len(detail_items) + self.detail_chunk_size - 1) // self.detail_chunk_size
            
            print(f"  Processing chunk {chunk_number}/{total_chunks} ({len(chunk)} details)...")

            # Create parallel runnables for this chunk
            detail_runnables = {}
            for detail_key, detail_data in chunk:
                detail_runnables[detail_key] = detail_data['chain']
                
            # Process this chunk with retry
            chunk_results = await self._process_detail_chunk(detail_runnables, document_text)
            
            # Merge results
            all_results.update(chunk_results)
            
            print(f"  ✅ Completed chunk {chunk_number}/{total_chunks}")

        return all_results

    @retry(
        stop=stop_after_attempt(6),
        wait=create_smart_wait_strategy(),
        retry=retry_if_exception_type((RateLimitError, Exception)),
        before_sleep=lambda retry_state: print(f"    Rate limit hit on chunk, retrying... (attempt {retry_state.attempt_number}/6)")
    )
    async def _process_detail_chunk(self, detail_runnables: Dict, document_text: str) -> Dict[str, AnalysisResult]:
        """Process a single chunk of details with retry logic."""
        # Create RunnableParallel for this chunk
        parallel_analysis = RunnableParallel(detail_runnables)
        input_data = {"document_text": document_text}

        try:
            # Execute parallel analysis for this chunk
            results = await parallel_analysis.ainvoke(input_data)
            return results
        except Exception as e:
            print(f"    Error analyzing detail chunk: {e}")
            # Re-raise to trigger retry mechanism
            raise e

    async def _analyze_segments_parallel(self, document_text: str) -> Dict[str, AnalysisResult]:
        """Execute parallel segment analysis with chunked processing and retry logic."""
        segment_items = list(self.segment_chains.items())
        
        # If segments are few, process all at once
        if len(segment_items) <= self.segment_chunk_size:
            print(f"Analyzing {len(segment_items)} segments in a single batch...")
            return await self._process_segment_chunk(dict(segment_items), document_text)

        # Otherwise, process in chunks
        print(f"Analyzing {len(segment_items)} segments in chunks of {self.segment_chunk_size}...")
        all_results = {}

        for i in range(0, len(segment_items), self.segment_chunk_size):
            chunk = segment_items[i:i + self.segment_chunk_size]
            chunk_number = (i // self.segment_chunk_size) + 1
            total_chunks = (len(segment_items) + self.segment_chunk_size - 1) // self.segment_chunk_size
            
            print(f"  Processing chunk {chunk_number}/{total_chunks} ({len(chunk)} segments)...")

            # Create parallel runnables for this chunk
            segment_runnables = dict(chunk)

            # Process this chunk with retry
            chunk_results = await self._process_segment_chunk(segment_runnables, document_text)
            
            # Merge results
            all_results.update(chunk_results)
            
            print(f"  ✅ Completed chunk {chunk_number}/{total_chunks}")

        return all_results

    @retry(
        stop=stop_after_attempt(6),
        wait=create_smart_wait_strategy(),
        retry=retry_if_exception_type((RateLimitError, Exception)),
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
            # Re-raise to trigger retry mechanism
            raise e

    async def analyze_document(self, document_path: str) -> Dict:
        """Analyze a single document for all segments and benefits in parallel with debug support."""

        # Load the document
        if not os.path.exists(document_path):
            raise FileNotFoundError(f"Document not found: {document_path}")

        with open(document_path, 'r', encoding='utf-8') as f:
            document_text = f.read()

        document_name = Path(document_path).stem

        print(f"=== Analyzing document: {document_name} ===" + (" (Debug Mode)" if self.debug_mode else ""))
        print(f"Document length: {len(document_text)} characters")
        print(f"Using chunk sizes: {self.segment_chunk_size} segments, {self.benefit_chunk_size} benefits, {self.detail_chunk_size} details per batch")

        # Prepare chunk size metadata for debug files
        chunk_sizes = {
            "segments": self.segment_chunk_size,
            "benefits": self.benefit_chunk_size,
            "details": self.detail_chunk_size
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
                if self.benefit_chains:
                    if self.debug_mode:
                        benefit_results, is_valid = load_debug_results(document_name, "benefits")

                    if benefit_results is None or not benefit_results:
                        print(f"🚀 Running benefit analysis ({len(self.benefit_chains)} benefits)...")
                        benefit_results = await self.analyze_benefits(document_text)
                        
                        if self.debug_mode:
                            save_debug_results(document_name, "benefits", benefit_results, chunk_sizes)

                    # Print benefit summary
                    print(f"\n=== Benefit Results Summary for {document_name} ===")
                    included_benefits = []
                    for benefit_key, result in benefit_results.items():
                        segment_name = self.benefit_chains[benefit_key]['segment_name']
                        benefit_name = self.benefit_chains[benefit_key]['benefit_info']['name']
                        status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                        print(f"  {segment_name} → {benefit_name}: {status}")
                        if result.is_included:
                            included_benefits.append(benefit_key)

                    # Step 3: Analyze details for included benefits
                    detail_results = {}
                    if included_benefits:
                        print(f"\nFound {len(included_benefits)} benefit(s). Proceeding to detail analysis...")

                        # Setup detail chains for included benefits
                        self.setup_detail_chains(benefit_results, segment_results)

                        # Analyze details with debug support
                        if self.detail_chains:
                            if self.debug_mode:
                                detail_results, is_valid = load_debug_results(document_name, "details")

                            if detail_results is None or not detail_results:
                                print(f"🚀 Running detail analysis ({len(self.detail_chains)} details)...")
                                detail_results = await self.analyze_details(document_text)
                                
                                if self.debug_mode:
                                    save_debug_results(document_name, "details", detail_results, chunk_sizes)

                            # Print detail summary
                            print(f"\n=== Detail Results Summary for {document_name} ===")
                            for detail_key, result in detail_results.items():
                                detail_data = self.detail_chains[detail_key]
                                segment_name = detail_data['segment_name']
                                benefit_key = detail_data['benefit_key']
                                benefit_name = self.benefit_chains[benefit_key]['benefit_info']['name']
                                detail_type = detail_data['detail_type']
                                detail_name = detail_data['detail_info']['name']
                                status = "✓ INCLUDED" if result.is_included else "✗ NOT FOUND"
                                print(f"  {segment_name} → {benefit_name} → {detail_type}: {detail_name} {status}")
                        else:
                            print("No details to analyze for the included benefits.")
                    else:
                        print("No benefits found. Skipping detail analysis.")
                else:
                    print("No benefits to analyze for the included segments.")
            else:
                print("No segments found. Skipping benefit and detail analysis.")

            # Step 4: Build hierarchical tree structure (unchanged)
            tree_structure = {"segments": []}

            # Process each segment
            for segment_name, segment_result in segment_results.items():
                if segment_result.is_included:
                    # Create segment with its data using taxonomy_item_name as key
                    segment_item = {
                        segment_name: {
                            "item_name": segment_result.item_name,
                            "is_included": segment_result.is_included,
                            "section_reference": segment_result.section_reference,
                            "full_text_part": segment_result.full_text_part,
                            "llm_summary": segment_result.llm_summary,
                            "description": segment_result.description,
                            "unit": segment_result.unit,
                            "value": segment_result.value,
                            "benefits": []
                        }
                    }

                    # Add benefits for this segment
                    for benefit_key, benefit_result in benefit_results.items():
                        if (benefit_result.is_included and 
                            self.benefit_chains[benefit_key]['segment_name'] == segment_name):
                            
                            benefit_name = self.benefit_chains[benefit_key]['benefit_info']['name']
                            benefit_item = {
                                benefit_name: {
                                    "item_name": benefit_result.item_name,
                                    "is_included": benefit_result.is_included,
                                    "section_reference": benefit_result.section_reference,
                                    "full_text_part": benefit_result.full_text_part,
                                    "llm_summary": benefit_result.llm_summary,
                                    "description": benefit_result.description,
                                    "unit": benefit_result.unit,
                                    "value": benefit_result.value,
                                    "limits": [],
                                    "conditions": [],
                                    "exclusions": []
                                }
                            }

                            # Add details (limits, conditions, exclusions) for this benefit
                            for detail_key, detail_result in detail_results.items():
                                if detail_key in self.detail_chains:
                                    detail_data = self.detail_chains[detail_key]
                                    if (detail_result.is_included and 
                                        detail_data['benefit_key'] == benefit_key):
                                        
                                        detail_type = detail_data['detail_type']
                                        detail_name = detail_data['detail_info']['name']
                                        detail_item = {
                                            detail_name: {
                                                "item_name": detail_result.item_name,
                                                "is_included": detail_result.is_included,
                                                "section_reference": detail_result.section_reference,
                                                "full_text_part": detail_result.full_text_part,
                                                "llm_summary": detail_result.llm_summary,
                                                "description": detail_result.description,
                                                "unit": detail_result.unit,
                                                "value": detail_result.value
                                            }
                                        }

                                        # Add to appropriate detail category
                                        if detail_type == "limits":
                                            benefit_item[benefit_name]["limits"].append(detail_item)
                                        elif detail_type == "conditions":
                                            benefit_item[benefit_name]["conditions"].append(detail_item)
                                        elif detail_type == "exclusions":
                                            benefit_item[benefit_name]["exclusions"].append(detail_item)

                            segment_item[segment_name]["benefits"].append(benefit_item)

                    tree_structure["segments"].append(segment_item)

            return tree_structure

        except Exception as e:
            print(f"Error analyzing document {document_name}: {e}")
            raise e

    def export_results(self, results: Dict, document_path: str):
        """Export results to JSON file."""

        document_name = Path(document_path).stem
        filename = f"{document_name}_analysis_results.json"

        # Convert Pydantic models to dictionaries in the new hierarchical structure
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

        serializable_results = convert_pydantic_to_dict(results)

        with open(filename, 'w', encoding='utf-8') as f:
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
                    print(f"Value: {segment_data.get('value', 'N/A')}")
                    
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
                                print(f"  Value: {benefit_data.get('value', 'N/A')}")
                                
                                full_text = benefit_data.get('full_text_part', 'N/A')
                                if benefit_data.get('is_included', False) and full_text != "N/A":
                                    print(f"  Full Text (first 300 chars): {full_text[:300]}...")

                                # Print details for this benefit
                                for detail_type in ['limits', 'conditions', 'exclusions']:
                                    details = benefit_data.get(detail_type, [])
                                    if details:
                                        print(f"\n    {detail_type.upper()} FOR {benefit_name.upper()}:")
                                        for detail_item in details:
                                            for detail_name, detail_data in detail_item.items():
                                                print(f"\n    --- {detail_type.upper()}: {detail_name.upper()} ---")
                                                print(f"    Included: {'✓ YES' if detail_data.get('is_included', False) else '✗ NO'}")
                                                print(f"    Section Reference: {detail_data.get('section_reference', 'N/A')}")
                                                print(f"    Description: {detail_data.get('description', 'N/A')}")
                                                print(f"    Summary: {detail_data.get('llm_summary', 'N/A')}")
                                                print(f"    Unit: {detail_data.get('unit', 'N/A')}")
                                                print(f"    Value: {detail_data.get('value', 'N/A')}")
                                                
                                                full_text = detail_data.get('full_text_part', 'N/A')
                                                if detail_data.get('is_included', False) and full_text != "N/A":
                                                    print(f"    Full Text (first 200 chars): {full_text[:200]}...")
        else:
            print(f"\nNo segments were found in the document.")


async def main():
    """Main function to handle command line arguments and run analysis."""

    parser = argparse.ArgumentParser(description="Analyze insurance documents for segment taxonomy items")
    parser.add_argument("document_path", help="Path to the insurance document (markdown format)")
    parser.add_argument("--export", "-e", action="store_true", help="Export results to JSON file")
    parser.add_argument("--detailed", "-d", action="store_true", help="Show detailed results")
    parser.add_argument("--no-cache", action="store_true", help="Disable caching for this run")
    parser.add_argument("--segment-chunks", type=int, default=8,
                       help="Number of segments to process in parallel per chunk (default: 8)")
    parser.add_argument("--benefit-chunks", type=int, default=8,
                       help="Number of benefits to process in parallel per chunk (default: 8)")
    parser.add_argument("--detail-chunks", type=int, default=3,
                       help="Number of details to process in parallel per chunk (default: 3). "
                            "Smaller default due to token-heavy responses.")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug mode: save intermediate results and auto-resume from existing debug files")
    parser.add_argument("--debug-clean", action="store_true",
                       help="Delete existing debug files before running (forces full re-run)")
    parser.add_argument("--debug-from", choices=["segments", "benefits", "details"],
                       help="Force re-run from specific tier (deletes subsequent debug files)")
    parser.add_argument("--seed-directus", action="store_true",
                       help="Seed analysis results to Directus after analysis")
    parser.add_argument("--product-id",
                       help="Existing dcm_product ID to seed data under (required with --seed-directus)")
    parser.add_argument("--dry-run-directus", action="store_true",
                       help="Dry run mode for Directus seeding - show what would be inserted")
    parser.add_argument("--cleanup-directus", action="store_true",
                       help="Clean up previously seeded data from Directus")

    args = parser.parse_args()

    # Disable cache if requested
    if args.no_cache:
        print("Caching disabled for this run")
        set_llm_cache(None)

    # Check if required environment variables are set
    if not os.getenv("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is not set")
        print("Please set your OpenAI API key in .env file")
        return 1

    if not os.getenv("GRAPHQL_AUTH_TOKEN"):
        print("Error: GRAPHQL_AUTH_TOKEN environment variable is not set")
        print("Please set your GraphQL auth token in .env file")
        return 1

    # Handle Directus cleanup if requested
    if args.cleanup_directus:
        if not args.product_id:
            print("Error: --product-id is required when using --cleanup-directus")
            print("Use: --product-id <dcm-product-id-to-cleanup>")
            return 1

        print("=" * 60)
        print(f"Cleaning up all data for product: {args.product_id}")
        print("=" * 60)

        success = cleanup_seeded_data(args.product_id)
        if success:
            print("✓ Successfully cleaned up all data from Directus!")
        else:
            print("✗ Failed to clean up data from Directus")
            return 1
        return 0


    try:
        # Handle debug flags
        document_name = Path(args.document_path).stem

        if args.debug_clean:
            print("🗑️  Cleaning all debug files...")
            clean_debug_files(document_name)

        if args.debug_from:
            print(f"🔄 Cleaning debug files from {args.debug_from} onwards...")
            clean_debug_files(document_name, args.debug_from)

        # Initialize analyzer with configurable chunk sizes for each tier
        analyzer = DocumentAnalyzer(
            segment_chunk_size=args.segment_chunks,
            benefit_chunk_size=args.benefit_chunks,
            detail_chunk_size=args.detail_chunks,
            debug_mode=args.debug
        )

        # Fetch taxonomy segments, benefits, and details from GraphQL
        print("Fetching segment taxonomy from GraphQL endpoint...")
        analyzer.segments = await analyzer.fetch_taxonomy_segments()
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
        print(f"Total available for analysis: {total_benefits} benefits, {total_details} details (limits/conditions/exclusions)")

        # Setup analysis chains
        analyzer.setup_analysis_chains()

        # Analyze the document
        results = await analyzer.analyze_document(args.document_path)

        # Export results if requested
        if args.export:
            analyzer.export_results(results, args.document_path)

        # Show detailed results if requested
        if args.detailed:
            analyzer.print_detailed_results(results, args.document_path)

        # Seed to Directus if requested
        if args.seed_directus:
            if not args.product_id:
                print("Error: --product-id is required when using --seed-directus")
                print("Use: --product-id <your-existing-dcm-product-id>")
                return 1

            print("\n" + "=" * 60)
            print("Seeding results to Directus...")
            print("=" * 60)

            # Convert results to the format expected by directus_seeder
            # The seeder expects a structure like: {document_name: {segments: [...]}}
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

            serialized_results = convert_pydantic_to_dict(results)
            seeder_format = {document_name: serialized_results}

            success = seed_to_directus(
                analysis_results=seeder_format,
                product_id=args.product_id,
                dry_run=args.dry_run_directus
            )

            if success:
                print("✓ Successfully seeded results to Directus!")
            else:
                print("✗ Failed to seed results to Directus")
                return 1

        print("\nAnalysis completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
