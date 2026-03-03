"""
GCC Presence Verification Service

Provides AI-powered verification for unmatched vehicle makes, models, and trims
to determine if they actually exist in the GCC market using Gemini with Google Search grounding.
"""

import asyncio
import nest_asyncio
import os
import re
from typing import List, Tuple, Optional, Callable, Dict, Any
import pandas as pd

try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


class GCCPresenceService:
    """
    Service for verifying vehicle presence in the GCC market using Gemini AI with Google Search.
    
    Features:
    - Auto-detection of vehicle dimension (make/model/trim) from input
    - Pre-search triage to filter out irrelevant or below-threshold inputs
    - Async batch verification with Google Search grounding
    - Rate limiting
    """

    # Classification Constants
    CLASS_BELOW_THRESHOLD = "BELOW_THRESHOLD"
    CLASS_IRRELEVANT = "IRRELEVANT"
    CLASS_EXISTS_SAME_NAME = "EXISTS_SAME_NAME"
    CLASS_SOLD_DIFFERENT_NAME = "SOLD_DIFFERENT_NAME"
    CLASS_NOT_IN_GCC = "NOT_IN_GCC"
    CLASS_ERROR = "ERROR"

    def __init__(self, max_concurrent_requests: int = 20, score_threshold: int = 60):
        """
        Initialize the GCC presence service.

        Args:
            max_concurrent_requests: Maximum number of concurrent API requests.
                                     Lower than standard verification to respect Search limits.
            score_threshold: Fuzzy match score above which we classify as BELOW_THRESHOLD
                             instead of performing a web search.
        """
        if not GEMINI_AVAILABLE:
            raise ImportError("google-genai package not installed. Install with: pip install google-genai")

        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")

        self.max_concurrent_requests = max_concurrent_requests
        self.score_threshold = score_threshold
        self.client = genai.Client(api_key=api_key)
        
        # Configure tool for Google Search grounding
        self.grounding_tool = types.Tool(
            google_search=types.GoogleSearch()
        )
        self.config = types.GenerateContentConfig(
            tools=[self.grounding_tool],
            temperature=0.1, # Keep it factual
        )
        self.model_name = "gemini-3-flash"

    @property
    def is_available(self) -> bool:
        """Check if Gemini functionality is available."""
        return GEMINI_AVAILABLE

    def _detect_dimension(self, row_dict: Dict[str, Any]) -> str:
        """Detect if the row is for Make, Model, or Trim based on available keys."""
        if 'Input Trim' in row_dict:
            return 'trim'
        elif 'Input Model' in row_dict:
            return 'model'
        elif 'Input Make' in row_dict:
            return 'make'
        return 'unknown'

    def triage_row(self, row_dict: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Since we now rely entirely on the AI, we always return True to proceed with the web search.
        However, if there's fundamentally missing data (like no input string detected at all), 
        we catch that here to save the API call.
        """
        dimension = self._detect_dimension(row_dict)
        if dimension == 'unknown':
            return False, self.CLASS_IRRELEVANT, "Could not detect vehicle dimension from data structure"
            
        return True, None, None

    def _build_prompt(self, dimension: str, row_dict: Dict[str, Any]) -> str:
        """Build the prompt for Gemini based on the vehicle dimension."""
        
        base_instruction = f"""
You are an automotive market expert for the GCC (Gulf Cooperation Council) region, particularly the UAE, Saudi Arabia, Qatar, Kuwait, Oman, and Bahrain.
Your first task is to determine if the user query is a valid vehicle {dimension}. If it is not a valid vehicle name (e.g. gibberish, incoherent), return IRRELEVANT without doing a web search.
If the input is valid, you need to definitively determine if it exists or is officially sold in the GCC market.
You will be provided with the user's input and optionally a "Best Match" and its "Score". The Best Match is the closest existing {dimension} we found in our system. The "Score Threshold" indicates our confidence limit.
If the input is coherent, but the Score is {self.score_threshold} or higher, it means the Best Match is highly likely what the user meant, but it was just below our auto-mapping threshold. In this case, return BELOW_THRESHOLD.

If the input is a valid vehicle {dimension} and it does NOT fall into IRRELEVANT or BELOW_THRESHOLD, use the Google Search tool to check current and historical market presence if you are unsure.

Respond ONLY with a valid JSON object matching this schema:
{{
  "classification": "One of: BELOW_THRESHOLD, IRRELEVANT, EXISTS_SAME_NAME, SOLD_DIFFERENT_NAME, NOT_IN_GCC",
  "comment": "A brief 1-2 sentence explanation of your finding."
}}

Definitions:
- BELOW_THRESHOLD: The input is coherent but a close 'Best Match' exists with a score above the threshold ({self.score_threshold}), indicating it's likely a typo of the Best Match.
- IRRELEVANT: The input is gibberish, incoherent, or not related to real vehicles.
- EXISTS_SAME_NAME: The vehicle {dimension} is or was officially sold in the GCC under this exact name.
- SOLD_DIFFERENT_NAME: The vehicle {dimension} is sold in the GCC, but under a different name (e.g., global name vs GCC name). Mention the GCC name in the comment.
- NOT_IN_GCC: The vehicle {dimension} has never been officially sold or present in the GCC market.
"""

        query = ""
        score = row_dict.get('Score', 'N/A')
        best_match = row_dict.get('Best Match', 'N/A')
        
        if dimension == 'make':
            make = row_dict.get('Input Make', '')
            query = f"Input Make: {make}\nBest Match: {best_match}\nScore: {score}\nScore Threshold: {self.score_threshold}"
        elif dimension == 'model':
            make = row_dict.get('Master Make', '')
            model = row_dict.get('Input Model', '')
            query = f"Master Make: {make}\nInput Model: {model}\nBest Match: {best_match}\nScore: {score}\nScore Threshold: {self.score_threshold}"
        elif dimension == 'trim':
            make = row_dict.get('Master Make', '')
            model = row_dict.get('Master Model', '')
            trim = row_dict.get('Input Trim', '')
            query = f"Master Make: {make}\nMaster Model: {model}\nInput Trim: {trim}\nBest Match: {best_match}\nScore: {score}\nScore Threshold: {self.score_threshold}"

        return f"{base_instruction}\n\nSearch Target:\n{query}"

    async def _process_single_search(self, row_dict: Dict[str, Any]) -> Tuple[str, str]:
        """
        Process a single row with Gemini + Search grounding.
        Returns: (classification, comment)
        """
        dimension = self._detect_dimension(row_dict)
        prompt = self._build_prompt(dimension, row_dict)
        
        try:
            # We add JSON format instruction but we don't strict JSON schema here 
            # because generate_content with tools + json_schema sometimes clashes in gemini-3-flash-preview.
            # The prompt instructs it to return JSON only.
            response = await self.client.aio.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=self.config
            )
            
            text = response.text.strip()
            # Clean markdown codeblocks if present
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
            
            import json
            try:
                result = json.loads(text)
                classification = result.get('classification', self.CLASS_ERROR)
                comment = result.get('comment', '')
                
                # Sanitize classification against allowed values
                allowed = [
                    self.CLASS_BELOW_THRESHOLD,
                    self.CLASS_IRRELEVANT,
                    self.CLASS_EXISTS_SAME_NAME, 
                    self.CLASS_SOLD_DIFFERENT_NAME, 
                    self.CLASS_NOT_IN_GCC
                ]
                if classification not in allowed:
                    
                    # Fallback parsing if the model was slightly off
                    if "SAME" in classification:
                        classification = self.CLASS_EXISTS_SAME_NAME
                    elif "DIFFERENT" in classification:
                        classification = self.CLASS_SOLD_DIFFERENT_NAME
                    elif "NOT" in classification:
                        classification = self.CLASS_NOT_IN_GCC
                    elif "BELOW" in classification:
                        classification = self.CLASS_BELOW_THRESHOLD
                    elif "IRRELEVANT" in classification or "GIBBERISH" in classification:
                        classification = self.CLASS_IRRELEVANT
                    else:
                        classification = self.CLASS_ERROR
                        
                return classification, comment
                
            except json.JSONDecodeError:
                return self.CLASS_ERROR, f"Failed to parse AI response: {text[:100]}..."
                
        except Exception as e:
            return self.CLASS_ERROR, f"API Error: {str(e)}"

    async def process_batch(
        self,
        df: pd.DataFrame,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> pd.DataFrame:
        """
        Process a batch of unmatched vehicles.
        
        Args:
            df: DataFrame containing unmatched vehicle data
            progress_callback: Optional callback(current, total, status_message)
            
        Returns:
            Annotated DataFrame with 'Classification' and 'Comment' columns
        """
        nest_asyncio.apply()
        
        result_df = df.copy()
        result_df['Classification'] = None
        result_df['Comment'] = None
        
        total_rows = len(df)
        processed = 0
        
        # 1. Triage all rows first (synchronous, fast)
        if progress_callback:
            progress_callback(0, total_rows, "Triaging rows to filter obvious matches...")
            
        search_tasks = []
        task_indices = []
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            needs_search, classification, comment = self. triage_row(row_dict)
            
            if not needs_search:
                result_df.at[idx, 'Classification'] = classification
                result_df.at[idx, 'Comment'] = comment
                processed += 1
            else:
                search_tasks.append(row_dict)
                task_indices.append(idx)
                
        # 2. Process web searches (asynchronous)
        if search_tasks:
            if progress_callback:
                progress_callback(processed, total_rows, f"Starting web search for {len(search_tasks)} vehicles...")
                
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            async def search_with_semaphore(idx, row_dict):
                async with semaphore:
                    nonlocal processed
                    classification, comment = await self._process_single_search(row_dict)
                    processed += 1
                    if progress_callback and processed % 5 == 0:  # Update UI every 5 completion
                        progress_callback(processed, total_rows, "Running Gemini + Google Search...")
                    return idx, classification, comment

            tasks = [
                search_with_semaphore(idx, row_dict)
                for idx, row_dict in zip(task_indices, search_tasks)
            ]
            
            results = await asyncio.gather(*tasks)
            
            # Update dataframe with search results
            for idx, classification, comment in results:
                result_df.at[idx, 'Classification'] = classification
                result_df.at[idx, 'Comment'] = comment
                
        if progress_callback:
            progress_callback(total_rows, total_rows, "Processing complete!")
            
        return result_df
