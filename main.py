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

# Load environment variables from .env file
load_dotenv()

# Set up in-memory caching for LangChain
set_llm_cache(InMemoryCache())


class AnalysisResult(BaseModel):
    """Schema for structured extraction of any analysis item (segment, benefit, limit, condition, exclusion) from a document."""
    section_reference: str = Field(description="Reference or identifier for the section.")
    full_text_part: str = Field(description="Full text of the section part.")
    llm_summary: str = Field(description="LLM-generated summary of the section.")
    item_name: str = Field(description="Name of the item being analyzed.")
    is_included: bool = Field(description="Indicates if the item is included.")
    description: str = Field(description="Description of what this item covers.")
    unit: str = Field(description="Unit of measurement if applicable (e.g., CHF, days, percentage).")
    value: str = Field(description="Specific value or amount found in the document.")


class DocumentAnalyzer:
    """Analyzes insurance documents for segment taxonomy items using GraphQL and OpenAI."""

    def __init__(self):
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
        self.llm = ChatOpenAI(
            model=openai_model,
            temperature=0,
        ).with_structured_output(AnalysisResult)

        self.segments = []
        self.segment_chains = {}
        self.benefits = {}  # Dict[segment_name, List[benefit_dict]]
        self.benefit_chains = {}
        self.details = {  # Dict[benefit_key, Dict[detail_type, List[detail_dict]]]
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

This context helps you understand that both the segment and benefit exist in the document. Now analyze the ENTIRE document for the specific {detail_type} described below.

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
        """Analyze all benefits for included segments in parallel."""

        if not self.benefit_chains:
            return {}

        print(f"Analyzing {len(self.benefit_chains)} benefits in parallel...")

        # Create parallel runnables for each benefit
        benefit_runnables = {}
        for benefit_key, benefit_data in self.benefit_chains.items():
            benefit_runnables[benefit_key] = benefit_data['chain']

        # Create RunnableParallel for all benefits
        parallel_analysis = RunnableParallel(benefit_runnables)

        # Prepare input data
        input_data = {"document_text": document_text}

        try:
            # Execute parallel analysis for all benefits
            results = await parallel_analysis.ainvoke(input_data)
            return results

        except Exception as e:
            print(f"Error analyzing benefits: {e}")
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
        """Analyze all details (limits/conditions/exclusions) for included benefits in parallel."""

        if not self.detail_chains:
            return {}

        print(f"Analyzing {len(self.detail_chains)} details (limits/conditions/exclusions) in parallel...")

        # Create parallel runnables for each detail
        detail_runnables = {}
        for detail_key, detail_data in self.detail_chains.items():
            detail_runnables[detail_key] = detail_data['chain']

        # Create RunnableParallel for all details
        parallel_analysis = RunnableParallel(detail_runnables)

        # Prepare input data
        input_data = {"document_text": document_text}

        try:
            # Execute parallel analysis for all details
            results = await parallel_analysis.ainvoke(input_data)
            return results

        except Exception as e:
            print(f"Error analyzing details: {e}")
            raise e

    async def analyze_document(self, document_path: str) -> Dict:
        """Analyze a single document for all segments and benefits in parallel."""

        # Load the document
        if not os.path.exists(document_path):
            raise FileNotFoundError(f"Document not found: {document_path}")

        with open(document_path, 'r', encoding='utf-8') as f:
            document_text = f.read()

        document_name = Path(document_path).stem

        print(f"=== Analyzing document: {document_name} ===")
        print(f"Document length: {len(document_text)} characters")
        print(f"Analyzing {len(self.segments)} segments in parallel...")
        print("Note: Results may be served from cache for faster response")

        # Step 1: Analyze segments
        segment_runnables = {}
        for segment_name, chain in self.segment_chains.items():
            segment_runnables[segment_name] = chain

        parallel_segment_analysis = RunnableParallel(segment_runnables)
        input_data = {"document_text": document_text}

        try:
            # Execute parallel analysis for all segments
            segment_results = await parallel_segment_analysis.ainvoke(input_data)

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

                # Analyze benefits
                if self.benefit_chains:
                    benefit_results = await self.analyze_benefits(document_text)

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

                        # Analyze details
                        if self.detail_chains:
                            detail_results = await self.analyze_details(document_text)

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

            # Step 4: Combine results
            combined_results = {
                'segments': segment_results,
                'benefits': benefit_results,
                'details': {
                    'limits': {},
                    'conditions': {},
                    'exclusions': {}
                },
                'included_segments': included_segments
            }

            # Organize detail results by type
            for detail_key, result in detail_results.items():
                detail_data = self.detail_chains[detail_key]
                detail_type = detail_data['detail_type']
                combined_results['details'][detail_type][detail_key] = result

            return combined_results

        except Exception as e:
            print(f"Error analyzing document {document_name}: {e}")
            raise e

    def export_results(self, results: Dict, document_path: str):
        """Export results to JSON file."""

        document_name = Path(document_path).stem
        filename = f"{document_name}_analysis_results.json"

        # Convert Pydantic models to dictionaries
        serializable_results = {
            'segments': {},
            'benefits': {},
            'details': {
                'limits': {},
                'conditions': {},
                'exclusions': {}
            },
            'included_segments': results.get('included_segments', [])
        }

        # Process segment results
        for segment_name, result in results.get('segments', {}).items():
            if hasattr(result, 'dict'):  # Pydantic model
                serializable_results['segments'][segment_name] = result.dict()
            else:
                serializable_results['segments'][segment_name] = result

        # Process benefit results
        for benefit_key, result in results.get('benefits', {}).items():
            if hasattr(result, 'dict'):  # Pydantic model
                serializable_results['benefits'][benefit_key] = result.dict()
            else:
                serializable_results['benefits'][benefit_key] = result

        # Process detail results
        for detail_type in ['limits', 'conditions', 'exclusions']:
            for detail_key, result in results.get('details', {}).get(detail_type, {}).items():
                if hasattr(result, 'dict'):  # Pydantic model
                    serializable_results['details'][detail_type][detail_key] = result.dict()
                else:
                    serializable_results['details'][detail_type][detail_key] = result

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({document_name: serializable_results}, f, indent=2, ensure_ascii=False)

        print(f"Results exported to {filename}")

    def print_detailed_results(self, results: Dict, document_path: str):
        """Print detailed results for the document."""

        document_name = Path(document_path).stem

        print(f"\n{'='*60}")
        print(f"DETAILED RESULTS: {document_name.upper()}")
        print(f"{'='*60}")

        # Print segment details
        print(f"\n{'='*40}")
        print("SEGMENTS")
        print(f"{'='*40}")

        for segment_name, result in results.get('segments', {}).items():
            print(f"\n--- SEGMENT: {segment_name.upper()} ---")
            if hasattr(result, 'is_included'):
                print(f"Included: {'✓ YES' if result.is_included else '✗ NO'}")
                print(f"Section Reference: {result.section_reference}")
                print(f"Summary: {result.llm_summary}")
                if result.is_included and result.full_text_part != "N/A":
                    print(f"Full Text (first 300 chars): {result.full_text_part[:300]}...")
            else:
                print(f"Result: {result}")

        # Print benefit details
        benefit_results = results.get('benefits', {})
        if benefit_results:
            print(f"\n{'='*40}")
            print("BENEFITS")
            print(f"{'='*40}")

            for benefit_key, result in benefit_results.items():
                segment_name = self.benefit_chains[benefit_key]['segment_name']
                benefit_name = self.benefit_chains[benefit_key]['benefit_info']['name']
                print(f"\n--- BENEFIT: {segment_name.upper()} → {benefit_name.upper()} ---")
                if hasattr(result, 'is_included'):
                    print(f"Included: {'✓ YES' if result.is_included else '✗ NO'}")
                    print(f"Section Reference: {result.section_reference}")
                    print(f"Summary: {result.llm_summary}")
                    print(f"Description: {result.description}")
                    print(f"Unit: {result.unit}")
                    print(f"Value: {result.value}")
                    if result.is_included and result.full_text_part != "N/A":
                        print(f"Full Text (first 300 chars): {result.full_text_part[:300]}...")
                else:
                    print(f"Result: {result}")
        else:
            print(f"\nNo benefits were analyzed (no segments were found in the document).")

        # Print detail results (limits, conditions, exclusions)
        detail_results = results.get('details', {})
        has_details = any(detail_results.get(detail_type, {}) for detail_type in ['limits', 'conditions', 'exclusions'])

        if has_details:
            print(f"\n{'='*40}")
            print("DETAILS (LIMITS, CONDITIONS, EXCLUSIONS)")
            print(f"{'='*40}")

            for detail_type in ['limits', 'conditions', 'exclusions']:
                type_results = detail_results.get(detail_type, {})
                if type_results:
                    print(f"\n--- {detail_type.upper()} ---")
                    for detail_key, result in type_results.items():
                        if detail_key in self.detail_chains:
                            detail_data = self.detail_chains[detail_key]
                            segment_name = detail_data['segment_name']
                            benefit_key = detail_data['benefit_key']
                            benefit_name = self.benefit_chains[benefit_key]['benefit_info']['name']
                            detail_name = detail_data['detail_info']['name']

                            print(f"\n{segment_name.upper()} → {benefit_name.upper()} → {detail_name.upper()}")
                            if hasattr(result, 'is_included'):
                                print(f"  Included: {'✓ YES' if result.is_included else '✗ NO'}")
                                print(f"  Section Reference: {result.section_reference}")
                                print(f"  Summary: {result.llm_summary}")
                                print(f"  Description: {result.description}")
                                print(f"  Unit: {result.unit}")
                                print(f"  Value: {result.value}")
                                if result.is_included and result.full_text_part != "N/A":
                                    print(f"  Full Text (first 200 chars): {result.full_text_part[:200]}...")
                            else:
                                print(f"  Result: {result}")
        else:
            print(f"\nNo details were analyzed (no benefits were found in the document).")


async def main():
    """Main function to handle command line arguments and run analysis."""

    parser = argparse.ArgumentParser(description="Analyze insurance documents for segment taxonomy items")
    parser.add_argument("document_path", help="Path to the insurance document (markdown format)")
    parser.add_argument("--export", "-e", action="store_true", help="Export results to JSON file")
    parser.add_argument("--detailed", "-d", action="store_true", help="Show detailed results")
    parser.add_argument("--no-cache", action="store_true", help="Disable caching for this run")

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

    try:
        # Initialize analyzer
        analyzer = DocumentAnalyzer()

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

        print("\nAnalysis completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
