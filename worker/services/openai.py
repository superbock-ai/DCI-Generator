"""
LLM Service for the DCI Generator using native LangChain structured output with LangSmith tracing
"""

import asyncio
import os
from typing import Any, Dict, Optional
from langchain_openai import ChatOpenAI
from langsmith import Client as LangSmithClient
from models import AnalysisResult
from utils import AnalyzerConfig, get_analysis_logger


class LLMService:
    """Service for managing LLM interactions with native structured output"""
    
    def __init__(self, config: AnalyzerConfig):
        """Initialize the LLM service with configuration and optional LangSmith tracing"""
        self.config = config
        self.logger = get_analysis_logger()

        # Configure LangSmith tracing if enabled
        self._setup_langsmith_tracing()

        # Create LLM with native structured output - clean and simple
        base_llm = ChatOpenAI(
            model=self.config.openai_model,
            temperature=0,
            api_key=self.config.openai_api_key
        )
        self._structured_llm = base_llm.with_structured_output(AnalysisResult)

        tracing_status = "enabled" if self.config.langsmith_tracing_enabled else "disabled"
        self.logger.debug_operation("llm_service_init",
                                   f"Initialized LLM service with model: {self.config.openai_model}, LangSmith tracing: {tracing_status}")

    def _setup_langsmith_tracing(self):
        """Configure LangSmith tracing environment variables"""
        if self.config.langsmith_tracing_enabled and self.config.langsmith_api_key:
            # Set environment variables for LangChain/LangSmith
            os.environ["LANGCHAIN_TRACING_V2"] = "true"
            os.environ["LANGCHAIN_API_KEY"] = self.config.langsmith_api_key
            if self.config.langsmith_project:
                os.environ["LANGCHAIN_PROJECT"] = self.config.langsmith_project

            # Initialize LangSmith client for additional functionality if needed
            try:
                self.langsmith_client = LangSmithClient(api_key=self.config.langsmith_api_key)
                self.logger.debug_operation("langsmith_init", f"LangSmith tracing enabled for project: {self.config.langsmith_project}")
            except Exception as e:
                self.logger.debug_operation("langsmith_init", f"LangSmith client initialization failed: {e}")
                self.langsmith_client = None
        else:
            self.langsmith_client = None
            if self.config.langsmith_tracing_enabled:
                self.logger.debug_operation("langsmith_init", "LangSmith tracing requested but API key not provided")

    async def analyze_with_structured_output(self, prompt: str, item_name: str = None) -> AnalysisResult:
        """
        Analyze text using native LangChain structured output with LangSmith tracing

        Args:
            prompt: The complete prompt to send to the LLM
            item_name: Optional item name for logging and tracing

        Returns:
            AnalysisResult: Structured analysis result
        """
        try:
            # Prepare metadata for LangSmith tracing
            metadata = {
                "item_name": item_name or "unknown_item",
                "model": self.config.openai_model,
                "analysis_type": "structured_output",
                "prompt_length": len(prompt)
            }

            # Use native structured output with enhanced tracing metadata
            if self.config.langsmith_tracing_enabled:
                # Include tracing tags and metadata for LangSmith
                result = await self._structured_llm.ainvoke(
                    prompt,
                    config={
                        "tags": ["dci-generator", "insurance-analysis", "structured-output"],
                        "metadata": metadata
                    }
                )
            else:
                result = await self._structured_llm.ainvoke(prompt)

            # Add the input prompt to the result
            if hasattr(result, 'input_prompt'):
                result.input_prompt = prompt

            if item_name:
                self.logger.analysis_item_result(item_name, result.is_included)

            return result
            
        except Exception as e:
            error_msg = f"LLM analysis failed for {item_name or 'unknown item'}: {str(e)}"
            self.logger.analysis_error("llm_analysis", item_name or "unknown", str(e))
            
            # Return default error result
            return AnalysisResult(
                item_name=item_name or "unknown_item",
                is_included=False,
                section_reference="",
                full_text_part="",
                llm_summary="Analysis failed due to LLM error",
                description="",
                unit="",
                value=None,
                unlimited=False,
                input_prompt=prompt
            )
    
    async def analyze_batch_with_structured_output(self, prompts: Dict[str, str], 
                                                  semaphore: Optional[asyncio.Semaphore] = None) -> Dict[str, AnalysisResult]:
        """
        Analyze multiple prompts in parallel with structured output
        
        Args:
            prompts: Dictionary mapping item identifiers to prompts
            semaphore: Optional semaphore for concurrency control
            
        Returns:
            Dictionary mapping item identifiers to analysis results
        """
        if not prompts:
            return {}
        
        self.logger.analysis_start("batch_analysis", len(prompts))
        
        async def analyze_single_with_semaphore(item_id: str, prompt: str) -> tuple[str, AnalysisResult]:
            if semaphore:
                async with semaphore:
                    result = await self.analyze_with_structured_output(prompt, item_id)
            else:
                result = await self.analyze_with_structured_output(prompt, item_id)
            return item_id, result
        
        # Execute all analyses in parallel
        tasks = [analyze_single_with_semaphore(item_id, prompt) 
                for item_id, prompt in prompts.items()]
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle any exceptions
        results = {}
        successful_count = 0
        
        for result in results_list:
            if isinstance(result, Exception):
                self.logger.analysis_error("batch_analysis", "unknown", str(result))
                continue
                
            item_id, analysis_result = result
            results[item_id] = analysis_result
            if analysis_result.is_included:
                successful_count += 1
        
        self.logger.analysis_complete("batch_analysis", successful_count, len(prompts))
        return results
    
    
    @property
    def model_name(self) -> str:
        """Get the current model name"""
        return self.config.openai_model