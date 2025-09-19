"""
PromptManager - Manages all prompts for the SimplifiedDocumentAnalyzer with exact wording from worker_main.py
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from simple_worker import TaxonomyItem


class PromptManager:
    """Manages all prompts for the SimplifiedDocumentAnalyzer with exact wording from worker_main.py"""
    
    def __init__(self):
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
   - Verlassen Sie sich NUR auf das, was schwarz auf weiß im Dokument steht
   - Bei Unklarheiten: Setzen Sie is_included auf false, anstatt zu raten
3. SCHWEIZERISCHE VERSICHERUNGSTERMINOLOGIE:
   - Verstehen Sie schweizerische Fachbegriffe und deren Bedeutungsunterschiede
   - Berücksichtigen Sie schweizer Rechtschreibung und Grammatik
   - Achten Sie auf branchenspezifische Begriffe und deren Kontextualisierung
4. STRUKTURIERTE EXTRAKTION:
   - Dokumentieren Sie präzise Fundstellen (Kapitel, Abschnitt, Seite)
   - Extrahieren Sie vollständige Textpassagen ohne Kürzungen
   - Erfassen Sie numerische Werte mit ihren Einheiten korrekt
   - Berücksichtigen Sie Währungen, Zeiträume und Mengenangaben
5. QUALITÄTSKONTROLLE:
   - Validieren Sie Ihre Findings gegen die Originaltextpassage
   - Überprüfen Sie logische Konsistenz Ihrer Extraktion
   - Stellen Sie sicher, dass alle Pflichtfelder korrekt befüllt sind
Die zu analysierenden AVB werden Ihnen als Markdown-formatierter Text bereitgestellt."""

    def create_segment_prompt(self, taxonomy_item: 'TaxonomyItem') -> str:
        """Create analysis prompt for a segment - exact copy from worker_main.py"""
        return f"""{self.base_system_prompt}
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
- Setzen Sie value auf: null (Segmente sind Abdeckungsbereiche, keine spezifischen Werte)
- Setzen Sie unlimited auf: false (Segmente sind Abdeckungsbereiche, nicht unbegrenzt)

Geben Sie EXAKT die Antwort im folgenden JSON-Format zurück:
{{
    "item_name": "{taxonomy_item.name}",
    "is_included": true/false,
    "section_reference": "Genauer Abschnitt/Seitenzahl wo gefunden",
    "full_text_part": "Vollständiger relevanter Textauszug aus dem Dokument",
    "llm_summary": "Kurze Zusammenfassung der gefundenen Deckung",
    "description": "Was genau ist abgedeckt",
    "unit": "N/A",
    "value": null,
    "unlimited": false
}}"""

    def create_benefit_prompt(self, taxonomy_item: 'TaxonomyItem', hierarchy_context: str = "") -> str:
        """Create analysis prompt for a benefit with segment context - exact copy from worker_main.py"""
        return f"""{self.base_system_prompt}
**ZIEL:**
Analysieren Sie, ob die unten beschriebene Leistung innerhalb des identifizierten Segments im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle relevanten Leistungsparameter.
{hierarchy_context}
Dieser Kontext hilft Ihnen zu verstehen, dass das Segment existiert, jedoch sollten Sie das GESAMTE Dokument nach der spezifischen Leistung durchsuchen.
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
Die Leistung ({taxonomy_item.name}) ist anwendbar DANN UND NUR DANN (IFF), wenn sie in semantischem Zusammenhang mit dem betreffenden Segment steht.
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
- Setzen Sie item_name immer auf: "{taxonomy_item.name}"
- Setzen Sie description auf: Eine Beschreibung der spezifischen Leistungsabdeckung
- Setzen Sie unit auf: Die Einheit aus der Taxonomie falls anwendbar: "{taxonomy_item.unit if taxonomy_item.unit else 'N/A'}"
- Setzen Sie value auf: Den numerischen Wert/Betrag falls gefunden, oder null wenn kein Wert
- Setzen Sie unlimited auf: true bei unbegrenzter Deckung, sonst false

**WERTE UND UNBEGRENZTE DECKUNG:**
- Wenn "unbegrenzt", "unlimited", "ohne Limit" erwähnt wird: value: null, unlimited: true
- Wenn numerische Werte erwähnt werden: value: [Zahl], unlimited: false
- Wenn kein Wert erwähnt wird: value: null, unlimited: false

**QUALITÄTSSICHERUNG FÜR LEISTUNGSANALYSE:**
1. **VOLLSTÄNDIGKEIT:** Prüfen Sie das gesamte Dokument systematisch nach der Leistung
2. **GENAUIGKEIT:** Verwenden Sie nur explizit im Text vorhandene Informationen
3. **SPRACHVERSTÄNDNIS:** Berücksichtigen Sie schweizerische Versicherungsterminologie
4. **STRUKTURIERTE EXTRAKTION:** Dokumentieren Sie präzise Fundstellen und Wortlaute
5. **VALIDIERUNG:** Bestätigen Sie den semantischen Zusammenhang mit dem Segment

Geben Sie EXAKT die Antwort im folgenden JSON-Format zurück:
{{
    "item_name": "{taxonomy_item.name}",
    "is_included": true/false,
    "section_reference": "Genauer Abschnitt/Seitenzahl wo gefunden",
    "full_text_part": "Vollständiger relevanter Textauszug aus dem Dokument",
    "llm_summary": "Kurze Zusammenfassung der gefundenen Deckung",
    "description": "Was genau ist abgedeckt",
    "unit": "{taxonomy_item.unit if taxonomy_item.unit else 'N/A'}",
    "value": null_or_number,
    "unlimited": true/false
}}"""

    def create_modifier_prompt(self, taxonomy_item: 'TaxonomyItem', hierarchy_context: str = "") -> str:
        """Create analysis prompt for a modifier with strict hierarchy context - enhanced from worker_main.py"""
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
        
        # Special instructions for numeric value extraction
        value_extraction_instructions = ""
        if modifier_type == "limit":
            value_extraction_instructions = """
**KRITISCHE ANWEISUNGEN FÜR NUMERISCHE WERTE UND UNBEGRENZTE DECKUNG:**
- Das 'value' Feld MUSS eine saubere Zahl enthalten (z.B. 50000, 1500, 250000) oder null sein
- Das 'unlimited' Feld ist ein boolean (true/false) und MUSS IMMER gesetzt werden
- Entfernen Sie Tausendertrennzeichen (Apostrophe) aus Schweizer Zahlen
- Verwenden Sie NUR den ERSTEN numerischen Wert aus dem Text
- Beispiele für korrekte Werte:
  * "50'000 für Einzelversicherung" → value: 50000, unlimited: false
  * "Maximum 1'500 CHF" → value: 1500, unlimited: false  
  * "Unbegrenzt" → value: null, unlimited: true
  * "Unlimited coverage" → value: null, unlimited: true
  * Kein Wert gefunden → value: null, unlimited: false
- NIEMALS beschreibende Texte im value-Feld verwenden!
- IMMER das unlimited-Feld auf true setzen bei Begriffen wie "unbegrenzt", "unlimited", "ohne Limit"!"""
        
        return f"""{self.base_system_prompt}
**ZIEL:**
Analysieren Sie, ob der unten beschriebene Modifikator (Bedingung, Limit oder Ausschluss) innerhalb der identifizierten Hierarchie im vorliegenden AVB-Dokument anwendbar ist. Falls ja, extrahieren Sie alle erforderlichen Modifikator-Parameter.
{hierarchy_context}
Dieser Kontext hilft Ihnen, die Hierarchie zu verstehen, für die Sie nun die Modifikatoren finden müssen. Der Modifikator ist ein wichtiger Aspekt der aktuellen Hierarchie. Analysieren Sie das GESAMTE Dokument nach dem spezifischen {modifier_type} unter STÄNDIGER Berücksichtigung der aktuellen Hierarchie.
**ZU ANALYSIERENDER MODIFIKATOR:**
- **Bezeichnung:** {taxonomy_item.name}
- **Beschreibung:** {taxonomy_item.description}
- **Alternative Begriffe:** {', '.join(taxonomy_item.aliases)}
- **Beispiele:** {', '.join(taxonomy_item.examples)}
- **Erwartete Einheit:** {taxonomy_item.unit if taxonomy_item.unit else 'N/A'}
- **Erwarteter Datentyp:** {taxonomy_item.data_type if taxonomy_item.data_type else 'N/A'}
**ANALYSEANWEISUNGEN FÜR {modifier_type_title}:**
{taxonomy_item.llm_instruction if taxonomy_item.llm_instruction else f'Suchen Sie nach {modifier_type}n im Zusammenhang mit der relevanten Hierarchie.'}
{hierarchy_requirement}{value_extraction_instructions}
**ANALYSEKRITERIEN:**
Der Modifikator ({taxonomy_item.name}) ist anwendbar DANN UND NUR DANN (IFF), wenn er in semantischem Zusammenhang mit der betreffenden Hierarchie steht.
- Wenn der Modifikator für eine andere Leistung/Segment im gesamten Dokument gilt, ist er HIER, in dieser Instanz, NICHT anwendbar
- Analysieren Sie das gesamte Versicherungsdokument und bestimmen Sie, ob dieser spezifische {modifier_type} erwähnt wird oder auf die Hierarchie zutrifft
- {modifier_type_title} können überall im Dokument erwähnt werden, nicht nur dort, wo die Hierarchie identifiziert wurde
**VORGEHEN BEI AUFFINDEN DES MODIFIKATORS:**
- Prüfen Sie, ob der {modifier_type} sich explizit auf ALLE Komponenten des Hierarchiekontexts bezieht
- Extrahieren Sie die relevante Abschnittsreferenz (z.B. Abschnittsnummer, Überschrift)  
- Erfassen Sie den vollständigen Wortlaut des relevanten Abschnitts, in dem der {modifier_type} beschrieben wird
- Erstellen Sie eine klare Zusammenfassung dessen, was dieser {modifier_type} spezifiziert
- Setzen Sie is_included auf true NUR wenn alle Hierarchieanforderungen erfüllt sind
- Setzen Sie description auf: Was dieser {modifier_type} abdeckt oder einschränkt
- Setzen Sie unit auf: Die gefundene Maßeinheit (z.B. CHF, Tage, Prozent) oder "N/A"
- Setzen Sie value auf: Saubere numerische Werte gemäß den Anweisungen oben oder null
- Setzen Sie unlimited auf: true bei unbegrenzter Deckung, sonst false
**VORGEHEN WENN MODIFIKATOR NICHT ERWÄHNT ODER NICHT ZUTREFFEND:**
- Setzen Sie is_included auf false
- Geben Sie eine kurze Erklärung in der Zusammenfassung, warum der {modifier_type} nicht anwendbar ist
- Verwenden Sie "N/A" für section_reference, full_text_part, description und unit
- Verwenden Sie null für value
- Verwenden Sie false für unlimited
**PFLICHTANGABEN:**
- Setzen Sie item_name immer auf: "{taxonomy_item.name}"
- Begründen Sie in llm_summary EXPLIZIT, warum der {modifier_type} für den spezifischen Hierarchiekontext anwendbar oder nicht anwendbar ist

Die Antwort wird automatisch im korrekten JSON-Format validiert. Stellen Sie sicher, dass alle Felder korrekt ausgefüllt sind."""