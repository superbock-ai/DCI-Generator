"""
Simplified DocumentAnalyzer - Process nested hierarchy with 15 concurrent requests
"""

import os
import asyncio
import json
from typing import Dict, List, Any, Optional
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from langchain_openai import ChatOpenAI
from pydantic import BaseModel


class AnalysisResult(BaseModel):
    """Simple analysis result model"""
    item_name: str
    is_included: bool
    section_reference: str = ""
    full_text_part: str = ""
    llm_summary: str = ""
    description: str = ""
    unit: str = ""
    value: str = ""
    input_prompt: str = ""  # Store the entire input prompt used for analysis


class TaxonomyItem(BaseModel):
    """Represents a taxonomy item with its relationship ID"""
    taxonomy_relationship_id: str
    taxonomy_item_id: str
    name: str
    category: str  # segment_type, benefit_type, limit_type, condition_type, exclusion_type
    description: str
    aliases: List[str]
    examples: List[str]
    llm_instruction: str = ""
    unit: str = ""
    data_type: str = ""
    
    @classmethod
    def create_from_graphql(cls, taxonomy_relationship_id: str, item: Dict) -> 'TaxonomyItem':
        """Create TaxonomyItem from GraphQL response, handling None values"""
        return cls(
            taxonomy_relationship_id=taxonomy_relationship_id,
            taxonomy_item_id=item['id'],
            name=item['taxonomy_item_name'],
            category=item['category'],
            description=item.get('description', ''),
            aliases=item.get('aliases', []),
            examples=item.get('examples', []),
            llm_instruction=item.get('llm_instruction', ''),
            unit=item.get('unit') or '',  # Handle None values
            data_type=item.get('data_type') or '',  # Handle None values
        )


class HierarchyNode:
    """Represents a node in the taxonomy hierarchy"""
    def __init__(self, taxonomy_item: TaxonomyItem, parent: Optional['HierarchyNode'] = None):
        self.taxonomy_item = taxonomy_item
        self.parent = parent
        self.children: List['HierarchyNode'] = []
        self.analysis_result: Optional[AnalysisResult] = None
    
    def add_child(self, child: 'HierarchyNode'):
        child.parent = self
        self.children.append(child)
    
    def get_hierarchy_context(self) -> str:
        """Build detailed context string showing the full hierarchy path with section references"""
        path = []
        current = self
        while current:
            if current.analysis_result and current.analysis_result.is_included:
                item_info = f"{current.taxonomy_item.name}: {current.analysis_result.llm_summary}"
                if current.analysis_result.section_reference:
                    item_info += f" (Fundstelle: {current.analysis_result.section_reference})"
                path.append(item_info)
            current = current.parent
        
        if not path:
            return ""
        
        path.reverse()  # Start from root
        context = "**ZU ANALYSIERENDER HIERARCHIEKONTEXT:**\n"
        for i, item in enumerate(path):
            indent = "  " * i
            context += f"{indent}- {item}\n"
        
        return context


class SimplifiedDocumentAnalyzer:
    """Simplified document analyzer processing nested hierarchy with 15 concurrent requests"""
    
    def __init__(self, dcm_id: str):
        self.dcm_id = dcm_id
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        # Initialize GraphQL client
        graphql_url = os.getenv("DIRECTUS_URL", "https://app-uat.quinsights.tech") + "/graphql"
        directus_token = os.getenv("DIRECTUS_AUTH_TOKEN")
        
        if not directus_token:
            raise ValueError("DIRECTUS_AUTH_TOKEN environment variable is required")
        
        transport = RequestsHTTPTransport(
            url=graphql_url,
            headers={"Authorization": f"Bearer {directus_token}"},
            use_json=True
        )
        self.graphql_client = Client(transport=transport, fetch_schema_from_transport=True)
        
        # Storage for hierarchy
        self.hierarchy_nodes: List[HierarchyNode] = []
        self.semaphore = asyncio.Semaphore(15)  # Limit to 15 concurrent requests
        
        # Base system prompt
        self.base_system_prompt = """Sie sind ein hochspezialisierter Experte für die Analyse von schweizerischen Versicherungs-AVB (Allgemeine Versicherungsbedingungen).

KRITISCHE ANALYSEPRINZIPIEN:

1. VOLLSTÄNDIGKEIT UND GRÜNDLICHKEIT:
   - Sie MÜSSEN das gesamte Dokument von der ersten bis zur letzten Seite systematisch durcharbeiten
   - Brechen Sie NIEMALS vorzeitig ab - relevante Informationen können auf der letzten Seite stehen

2. ABSOLUTE GENAUIGKEIT:
   - Analysieren Sie AUSSCHLIESSLICH auf Basis der im Dokument explizit vorhandenen Informationen
   - Machen Sie KEINE Annahmen oder Interpretationen über nicht explizit genannte Sachverhalte

Die zu analysierenden AVB werden Ihnen als Markdown-formatierter Text bereitgestellt."""

    def fetch_taxonomy_data(self) -> None:
        """Fetch complete taxonomy hierarchy from GraphQL and build nested structure"""
        query_file = os.path.join(os.path.dirname(__file__), 'graphql', 'GetCompleteTaxonomyHierarchy.graphql')
        with open(query_file, 'r') as f:
            query_text = f.read()
        
        query = gql(query_text)
        variables = {"dcm_id": self.dcm_id}
        result = self.graphql_client.execute(query, variable_values=variables)
        
        print(f"Processing taxonomy data for DCM: {self.dcm_id}")
        
        # Process the taxonomy hierarchy and build nested structure
        for product_item in result['taxonomy_items']:
            if product_item['category'] == 'product_type':
                # Create product node (root)
                product_taxonomy = TaxonomyItem.create_from_graphql("product", product_item)
                product_node = HierarchyNode(product_taxonomy)
                
                # Process product-level modifiers
                self._process_product_modifiers(product_node, product_item)
                
                # Process segments
                for segment_rel in product_item['parent_relationships']:
                    segment_item = segment_rel['related_taxonomy_item']
                    if segment_item['category'] == 'segment_type':
                        self._process_segment(product_node, segment_rel, segment_item)
                
                self.hierarchy_nodes.append(product_node)
        
        # Count all nodes for reporting
        total_nodes = sum(self._count_nodes(node) for node in self.hierarchy_nodes)
        print(f"Built hierarchy with {total_nodes} total nodes")

    def _process_product_modifiers(self, product_node: HierarchyNode, product_item: Dict) -> None:
        """Process product-level modifiers (conditions, limits, exclusions)"""
        for modifier_group in ['product_conditions', 'product_limits', 'product_exclusions']:
            for modifier_rel in product_item.get(modifier_group, []):
                modifier_item = modifier_rel['related_taxonomy_item']
                taxonomy_item = TaxonomyItem.create_from_graphql(modifier_rel['id'], modifier_item)
                modifier_node = HierarchyNode(taxonomy_item)
                product_node.add_child(modifier_node)

    def _process_segment(self, product_node: HierarchyNode, segment_rel: Dict, segment_item: Dict) -> None:
        """Process a segment and its related benefits and modifiers"""
        # Create segment node
        segment_taxonomy = TaxonomyItem.create_from_graphql(segment_rel['id'], segment_item)
        segment_node = HierarchyNode(segment_taxonomy)
        product_node.add_child(segment_node)
        
        # Process segment-level modifiers
        for modifier_group in ['segment_conditions', 'segment_limits', 'segment_exclusions']:
            for modifier_rel in segment_item.get(modifier_group, []):
                modifier_item = modifier_rel['related_taxonomy_item']
                taxonomy_item = TaxonomyItem.create_from_graphql(modifier_rel['id'], modifier_item)
                modifier_node = HierarchyNode(taxonomy_item)
                segment_node.add_child(modifier_node)
        
        # Process benefits for this segment
        for benefit_rel in segment_item.get('parent_relationships', []):
            benefit_item = benefit_rel['related_taxonomy_item']
            if benefit_item['category'] == 'benefit_type':
                self._process_benefit(segment_node, benefit_rel, benefit_item)

    def _process_benefit(self, segment_node: HierarchyNode, benefit_rel: Dict, benefit_item: Dict) -> None:
        """Process a benefit and its modifiers"""
        # Create benefit node
        benefit_taxonomy = TaxonomyItem.create_from_graphql(benefit_rel['id'], benefit_item)
        benefit_node = HierarchyNode(benefit_taxonomy)
        segment_node.add_child(benefit_node)
        
        # Process benefit-level modifiers
        for modifier_group in ['benefit_conditions', 'benefit_limits', 'benefit_exclusions']:
            for modifier_rel in benefit_item.get(modifier_group, []):
                modifier_item = modifier_rel['related_taxonomy_item']
                taxonomy_item = TaxonomyItem.create_from_graphql(modifier_rel['id'], modifier_item)
                modifier_node = HierarchyNode(taxonomy_item)
                benefit_node.add_child(modifier_node)

    def _count_nodes(self, node: HierarchyNode) -> int:
        """Count total nodes in subtree"""
        return 1 + sum(self._count_nodes(child) for child in node.children)

    def _count_modifiers_recursive(self, nodes: List[HierarchyNode]) -> int:
        """Count all modifier nodes in a list of nodes recursively"""
        count = 0
        for node in nodes:
            # Count this node if it's a modifier
            if node.taxonomy_item.category in ['limit_type', 'condition_type', 'exclusion_type']:
                count += 1
            # Count modifiers in children
            count += self._count_modifiers_recursive(node.children)
        return count

    def _create_fresh_llm(self) -> ChatOpenAI:
        """Create a fresh LLM instance to avoid cache contamination"""
        return ChatOpenAI(
            model=self.openai_model,
            temperature=0,
            model_kwargs={"response_format": {"type": "json_object"}}
        )

    def _create_segment_prompt(self, taxonomy_item: TaxonomyItem) -> str:
        """Create analysis prompt for a segment"""
        prompt = f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob das unten beschriebene Versicherungssegment im vorliegenden AVB-Dokument abgedeckt ist. Falls ja, extrahieren Sie alle relevanten Segmentparameter.

**ZU ANALYSIERENDES SEGMENT:**
- **Bezeichnung:** {taxonomy_item.name}
- **Beschreibung:** {taxonomy_item.description}
- **Alternative Begriffe:** {', '.join(taxonomy_item.aliases)}
- **Beispiele:** {', '.join(taxonomy_item.examples)}

**ANALYSEANWEISUNGEN:**
{taxonomy_item.llm_instruction}

**ANALYSEKRITERIEN:**
Das Segment ({taxonomy_item.name}) ist abgedeckt DANN UND NUR DANN (IFF), wenn es explizit im Versicherungsdokument erwähnt oder beschrieben wird.
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
- Setzen Sie item_name immer auf: "{taxonomy_item.name}"
- Setzen Sie description auf: Eine kurze Beschreibung der Segmentabdeckung
- Setzen Sie unit auf: "N/A" (Segmente haben typischerweise keine Einheiten)
- Setzen Sie value auf: "0.0" (Segmente sind Abdeckungsbereiche, keine spezifischen Werte)

Geben Sie EXAKT die Antwort im folgenden JSON-Format zurück:
{{
    "item_name": "{taxonomy_item.name}",
    "is_included": true/false,
    "section_reference": "Genauer Abschnitt/Seitenzahl wo gefunden",
    "full_text_part": "Vollständiger relevanter Textauszug aus dem Dokument",
    "llm_summary": "Kurze Zusammenfassung der gefundenen Deckung",
    "description": "Was genau ist abgedeckt",
    "unit": "N/A",
    "value": "0.0"
}}"""
        return prompt

    def _create_benefit_prompt(self, taxonomy_item: TaxonomyItem, hierarchy_context: str = "") -> str:
        """Create analysis prompt for a benefit with hierarchy context"""
        prompt = f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob die unten beschriebene Leistung im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle relevanten Leistungsparameter.

{hierarchy_context}

**ZU ANALYSIERENDE LEISTUNG:**
- **Bezeichnung:** {taxonomy_item.name}
- **Beschreibung:** {taxonomy_item.description}
- **Alternative Begriffe:** {', '.join(taxonomy_item.aliases)}
- **Beispiele:** {', '.join(taxonomy_item.examples)}
- **Einheit:** {taxonomy_item.unit if taxonomy_item.unit else 'N/A'}
- **Datentyp:** {taxonomy_item.data_type if taxonomy_item.data_type else 'N/A'}

**ANALYSEANWEISUNGEN:**
{taxonomy_item.llm_instruction}

**ANALYSEKRITERIEN:**
Die Leistung ({taxonomy_item.name}) ist anwendbar DANN UND NUR DANN (IFF), wenn sie explizit im Versicherungsdokument erwähnt oder beschrieben wird.
- Leistungen können überall im Dokument erwähnt werden
- Wenn eine Leistung für ein Segment anwendbar ist, steht sie normalerweise in Verbindung mit Abdeckungsmodifikatoren (Bedingungen, Limits und Ausschlüsse), die in einem späteren Stadium extrahiert werden

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
- Setzen Sie item_name immer auf: "{taxonomy_item.name}"
- Setzen Sie description auf: Eine Beschreibung der spezifischen Leistungsabdeckung
- Setzen Sie unit auf: "{taxonomy_item.unit if taxonomy_item.unit else 'N/A'}"
- Setzen Sie value auf: Den spezifischen Wert/Betrag, der im Dokument für diese Leistung gefunden wurde

Geben Sie EXAKT die Antwort im folgenden JSON-Format zurück:
{{
    "item_name": "{taxonomy_item.name}",
    "is_included": true/false,
    "section_reference": "Genauer Abschnitt/Seitenzahl wo gefunden",
    "full_text_part": "Vollständiger relevanter Textauszug aus dem Dokument",
    "llm_summary": "Kurze Zusammenfassung der gefundenen Deckung",
    "description": "Was genau ist abgedeckt",
    "unit": "{taxonomy_item.unit if taxonomy_item.unit else 'N/A'}",
    "value": "Spezifischer Betrag oder Wert falls gefunden"
}}"""
        return prompt

    def _create_modifier_prompt(self, taxonomy_item: TaxonomyItem, hierarchy_context: str = "") -> str:
        """Create analysis prompt for a modifier with strict hierarchy context requirements"""
        modifier_type = taxonomy_item.category.replace('_type', '')  # limit_type -> limit
        modifier_type_title = modifier_type.upper()
        
        # Extract hierarchy component names for strict checking
        hierarchy_components = []
        if hierarchy_context:
            lines = hierarchy_context.split('\n')
            for line in lines:
                if line.strip().startswith('- ') and ':' in line:
                    component_name = line.split(':')[0].replace('- ', '').strip()
                    if component_name != 'product':  # Skip product level
                        hierarchy_components.append(component_name)
        
        hierarchy_requirement = ""
        if hierarchy_components:
            component_list = " UND ".join(f"'{comp}'" for comp in hierarchy_components)
            hierarchy_requirement = f"""
**WICHTIG**: Ermitteln Sie den zu analysierenden Modifikator AUSSCHLIESSLICH innerhalb des oben genannten Hierarchiekontexts.

Der Modifikator ({taxonomy_item.name}) ist anwendbar DANN UND NUR DANN (IFF), wenn ALLE der folgenden Bedingungen erfüllt sind:
1. Der Modifikator wird im Dokument explizit erwähnt.
2. Die Erwähnung des Modifikators steht in einem direkten und untrennbaren semantischen Zusammenhang mit ALLEN Teilen des ZU ANALYSIERENDEN HIERARCHIEKONTEXTS. Das bedeutet, der {modifier_type} muss sich explizit auf {component_list} beziehen.
3. Es dürfen KEINE {modifier_type}e oder ähnliche Modifikatoren berücksichtigt werden, die für andere Module oder Leistungsteile der Versicherung gelten, die nicht explizit im definierten Hierarchiekontext ({", ".join(hierarchy_components)}) liegen.

**STRIKTE AUSSCHLUSSKRITERIEN:**
- Ignorieren Sie {modifier_type}e, die sich auf andere Segmente oder Leistungen beziehen
- Ignorieren Sie allgemeine {modifier_type}e, die nicht spezifisch für den Hierarchiekontext gelten
- Berücksichtigen Sie NUR {modifier_type}e, die explizit und ausschließlich für den gesamten Hierarchiekontext relevant sind"""
        
        prompt = f"""{self.base_system_prompt}

**ZIEL:**
Analysieren Sie, ob der unten beschriebene Modifikator (Bedingung, Limit oder Ausschluss) im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle erforderlichen Modifikator-Parameter.

{hierarchy_context}

**ZU ANALYSIERENDER MODIFIKATOR:**
- **Bezeichnung:** {taxonomy_item.name}
- **Beschreibung:** {taxonomy_item.description}
- **Alternative Begriffe:** {', '.join(taxonomy_item.aliases)}
- **Beispiele:** {', '.join(taxonomy_item.examples)}
- **Erwartete Einheit:** {taxonomy_item.unit if taxonomy_item.unit else 'N/A'}
- **Erwarteter Datentyp:** {taxonomy_item.data_type if taxonomy_item.data_type else 'N/A'}

**ANALYSEANWEISUNGEN FÜR {modifier_type_title}:**
{taxonomy_item.llm_instruction if taxonomy_item.llm_instruction else f'Suchen Sie nach {modifier_type}n im Zusammenhang mit der relevanten Leistung oder dem Segment.'}

{hierarchy_requirement}

**VORGEHEN BEI AUFFINDEN DES MODIFIKATORS:**
- Prüfen Sie, ob der {modifier_type} sich explizit auf ALLE Komponenten des Hierarchiekontexts bezieht
- Extrahieren Sie die relevante Abschnittsreferenz (z.B. Abschnittsnummer, Überschrift)
- Erfassen Sie den vollständigen Wortlaut des relevanten Abschnitts, in dem der {modifier_type} beschrieben wird
- Erstellen Sie eine klare Zusammenfassung dessen, was dieser {modifier_type} spezifiziert
- Setzen Sie is_included auf true NUR wenn alle Hierarchieanforderungen erfüllt sind
- Setzen Sie description auf: Was dieser {modifier_type} abdeckt oder einschränkt
- Setzen Sie unit auf: Die gefundene Maßeinheit (z.B. CHF, Tage, Prozent) oder "N/A"
- Setzen Sie value auf: Den spezifischen Wert, Betrag oder die Bedingung, die im Dokument gefunden wurde

**VORGEHEN WENN MODIFIKATOR NICHT ERWÄHNT ODER NICHT ZUTREFFEND:**
- Setzen Sie is_included auf false
- Geben Sie eine kurze Erklärung in der Zusammenfassung, warum der {modifier_type} nicht anwendbar ist
- Verwenden Sie "N/A" für section_reference, full_text_part, description und unit
- Verwenden Sie "0.0" für value

**PFLICHTANGABEN:**
- Setzen Sie item_name immer auf: "{taxonomy_item.name}"
- Begründen Sie in llm_summary EXPLIZIT, warum der {modifier_type} für den spezifischen Hierarchiekontext anwendbar oder nicht anwendbar ist

Geben Sie EXAKT die Antwort im folgenden JSON-Format zurück:
{{
    "item_name": "{taxonomy_item.name}",
    "is_included": true/false,
    "section_reference": "Genauer Abschnitt/Seitenzahl wo gefunden",
    "full_text_part": "Vollständiger relevanter Textauszug aus dem Dokument",
    "llm_summary": "Begründung warum dieser {modifier_type} für den Hierarchiekontext anwendbar/nicht anwendbar ist",
    "description": "Was dieser {modifier_type} abdeckt oder einschränkt",
    "unit": "{taxonomy_item.unit if taxonomy_item.unit else 'N/A'}",
    "value": "Spezifischer Wert, Betrag oder Bedingung"
}}"""
        return prompt

    async def _analyze_single_node(self, node: HierarchyNode, document_text: str) -> None:
        """Analyze a single node with semaphore control"""
        async with self.semaphore:
            try:
                taxonomy_item = node.taxonomy_item
                llm = self._create_fresh_llm()
                
                # Build hierarchy context from parent nodes
                hierarchy_context = node.get_hierarchy_context()
                
                # Select appropriate prompt based on category
                if taxonomy_item.category == 'segment_type':
                    prompt_text = self._create_segment_prompt(taxonomy_item)
                elif taxonomy_item.category == 'benefit_type':
                    prompt_text = self._create_benefit_prompt(taxonomy_item, hierarchy_context)
                elif taxonomy_item.category in ['limit_type', 'condition_type', 'exclusion_type']:
                    prompt_text = self._create_modifier_prompt(taxonomy_item, hierarchy_context)
                else:
                    # Fallback for any unknown category
                    prompt_text = self._create_segment_prompt(taxonomy_item)
                
                # Combine prompt with document
                full_prompt = f"{prompt_text}\n\nZu analysierendes AVB-Dokument:\n\n{document_text}"
                
                response = await llm.ainvoke(full_prompt)
                result_data = json.loads(response.content)
                
                # Add the input prompt to the result data
                result_data["input_prompt"] = full_prompt
                
                # Validate and create AnalysisResult
                node.analysis_result = AnalysisResult(**result_data)
                
                print(f"✓ Analyzed: {taxonomy_item.name} -> {node.analysis_result.is_included}")
                
            except Exception as e:
                print(f"✗ Error analyzing {node.taxonomy_item.name}: {e}")
                # Set default result on error
                node.analysis_result = AnalysisResult(
                    item_name=node.taxonomy_item.name,
                    is_included=False,
                    section_reference="",
                    full_text_part="",
                    llm_summary="Analysis failed",
                    description="",
                    unit="",
                    value="",
                    input_prompt=f"Error occurred while generating prompt for {node.taxonomy_item.name}"
                )

    async def _analyze_hierarchy_recursive(self, nodes: List[HierarchyNode], document_text: str) -> None:
        """Analyze hierarchy recursively, depth-first to complete nested structures"""
        if not nodes:
            return
        
        # Create tasks for all nodes at this level
        tasks = [self._analyze_single_node(node, document_text) for node in nodes]
        
        # Wait for all nodes at this level to complete
        await asyncio.gather(*tasks)
        
        # Now process children of nodes that were found to be included
        child_tasks = []
        skipped_modifiers = 0
        
        for node in nodes:
            if node.analysis_result and node.analysis_result.is_included and node.children:
                print(f"📋 {node.taxonomy_item.name} is included - analyzing {len(node.children)} child modifiers")
                # Process all children of included nodes (benefits/modifiers)
                child_tasks.append(self._analyze_hierarchy_recursive(node.children, document_text))
            elif node.children:
                # Count skipped modifiers for reporting
                skipped_count = self._count_modifiers_recursive(node.children)
                skipped_modifiers += skipped_count
                if skipped_count > 0:
                    print(f"⏭️  {node.taxonomy_item.name} not included - skipping {skipped_count} related modifiers")
        
        if skipped_modifiers > 0:
            print(f"🚫 Total skipped modifiers due to parent exclusion: {skipped_modifiers}")
        
        # Process all child hierarchies in parallel
        if child_tasks:
            await asyncio.gather(*child_tasks)

    def _collect_results_recursive(self, nodes: List[HierarchyNode], results: Dict[str, Dict]) -> None:
        """Collect results from hierarchy recursively"""
        for node in nodes:
            if node.analysis_result and node.analysis_result.is_included:
                category_key = node.taxonomy_item.category.replace('_type', 's')  # segment_type -> segments
                if category_key == 'segments':
                    category_key = 'segments'
                elif category_key == 'benefits':
                    category_key = 'benefits'
                else:
                    # All modifiers go into 'modifiers' category
                    category_key = 'modifiers'
                
                if category_key not in results:
                    results[category_key] = {}
                
                results[category_key][node.taxonomy_item.taxonomy_relationship_id] = {
                    "taxonomy_relationship_id": node.taxonomy_item.taxonomy_relationship_id,
                    "item_name": node.analysis_result.item_name,
                    "is_included": node.analysis_result.is_included,
                    "section_reference": node.analysis_result.section_reference,
                    "full_text_part": node.analysis_result.full_text_part,
                    "llm_summary": node.analysis_result.llm_summary,
                    "description": node.analysis_result.description,
                    "unit": node.analysis_result.unit,
                    "value": node.analysis_result.value,
                    "input_prompt": node.analysis_result.input_prompt
                }
            
            # Always process children regardless of parent inclusion status for complete results
            self._collect_results_recursive(node.children, results)

    async def analyze_document(self, document_text: str) -> Dict[str, Any]:
        """Main analysis function - analyze document against nested hierarchy"""
        print("Starting hierarchical document analysis...")
        print(f"Using semaphore with limit of 15 concurrent requests")
        print(f"📏 Conditional analysis: Modifiers only analyzed for included segments/benefits")
        
        # Start recursive analysis from root nodes
        await self._analyze_hierarchy_recursive(self.hierarchy_nodes, document_text)
        
        # Collect all results
        results = {}
        self._collect_results_recursive(self.hierarchy_nodes, results)
        
        # Ensure all categories exist
        for category in ['segments', 'benefits', 'modifiers']:
            if category not in results:
                results[category] = {}
        
        print(f"\n🎯 Hierarchical Analysis Complete:")
        print(f"  - {len(results['segments'])} segments found and included")
        print(f"  - {len(results['benefits'])} benefits found and included") 
        print(f"  - {len(results['modifiers'])} modifiers found and included")
        print(f"💡 Note: Only modifiers for included segments/benefits were analyzed")
        
        return results


# Simple test function
async def main():
    """Test the simplified analyzer"""
    analyzer = SimplifiedDocumentAnalyzer(dcm_id="test")
    analyzer.fetch_taxonomy_data()
    
    # Test with a simple document
    test_document = "This is a test insurance document..."
    results = await analyzer.analyze_document(test_document)
    
    print("\nResults:", json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())