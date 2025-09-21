"""
LLM Service for the DCI Generator using native LangChain structured output
LangSmith tracing is automatic when environment variables are set
"""

import asyncio
import os
from typing import Any, Dict, Optional
from langchain_openai import ChatOpenAI
from models import AnalysisResult
from utils import AnalyzerConfig, get_analysis_logger


class LLMService:
    """Service for managing LLM interactions with native structured output"""

    def __init__(self, config: AnalyzerConfig):
        """Initialize the LLM service with configuration"""
        self.config = config
        self.logger = get_analysis_logger()
        self.thread_id = None  # Will be set by analysis job

        # Set up LangSmith environment variables if configured
        self._setup_langsmith_environment()

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

    def set_thread_context(self, thread_id: str, product_id: str = None):
        """Set the thread context for grouping related analysis calls"""
        self.thread_id = thread_id
        self.product_id = product_id
        if self.config.langsmith_tracing_enabled:
            self.logger.debug_operation("thread_context", f"Set thread context: {thread_id} for product: {product_id}")

    def _setup_langsmith_environment(self):
        """Set up LangSmith environment variables for automatic tracing"""
        if self.config.langsmith_tracing_enabled and self.config.langsmith_api_key:
            # LangChain automatically detects these environment variables
            os.environ["LANGCHAIN_TRACING_V2"] = "true"
            os.environ["LANGSMITH_API_KEY"] = self.config.langsmith_api_key
            if self.config.langsmith_project:
                os.environ["LANGCHAIN_PROJECT"] = self.config.langsmith_project

            self.logger.debug_operation("langsmith_init", f"LangSmith automatic tracing enabled for project: {self.config.langsmith_project}")
        elif self.config.langsmith_tracing_enabled:
            self.logger.debug_operation("langsmith_init", "LangSmith tracing requested but API key not provided")

    async def analyze_with_structured_output(self, prompt: str, item_name: str = None) -> AnalysisResult:
        """
        Analyze text using native LangChain structured output
        LangSmith tracing is automatic when environment variables are set

        Args:
            prompt: The complete prompt to send to the LLM
            item_name: Optional item name for logging

        Returns:
            AnalysisResult: Structured analysis result
        """
        try:
            # LangChain automatically traces when LANGCHAIN_TRACING_V2=true
            # Add custom run name and metadata for better tracing
            run_name = f"analyze_{item_name}" if item_name else "analyze_document"
            metadata = {
                "item_name": item_name or "unknown_item",
                "model": self.config.openai_model,
                "analysis_type": "structured_output",
                "prompt_length": len(prompt)
            }

            # Add thread context for grouping related analysis calls
            if self.thread_id:
                metadata["thread_id"] = self.thread_id
                if hasattr(self, 'product_id') and self.product_id:
                    metadata["product_id"] = self.product_id

            result = await self._structured_llm.ainvoke(
                prompt,
                config={
                    "run_name": run_name,
                    "tags": ["dci-generator", "insurance-analysis", "structured-output"],
                    "metadata": metadata
                }
            )

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