"""
Arabic Translation Service

Provides Arabic to English translation capabilities for vehicle specifications
using OpenAI's API with automotive-specific prompts and batch processing support.
"""

import re
import asyncio
import nest_asyncio
import os
from typing import Dict, List, Optional, Callable
import pandas as pd

# Translation imports (optional)
try:
    from google import genai
    TRANSLATION_AVAILABLE = True
except ImportError:
    TRANSLATION_AVAILABLE = False


class ArabicTranslationService:
    """
    Service for translating Arabic vehicle names to English using Gemini API.
    
    Features:
    - Async batch translation with rate limiting
    - Automotive-specific translation prompts
    - Progress tracking callbacks
    - Arabic text detection utilities
    - Translation caching support
    """
    
    def __init__(self, api_key: Optional[str] = None, max_concurrent_requests: int = 20):
        """
        Initialize the translation service.
        
        Args:
            api_key: Optional Gemini API key. If not provided, reads from GEMINI_API_KEY env var
            max_concurrent_requests: Maximum number of concurrent API requests
        """
        if not TRANSLATION_AVAILABLE:
            print("google-genai package not installed. Translation features won't be available.")
            self.client = None
        else:
            api_key = api_key or os.getenv('GEMINI_API_KEY')
            if not api_key:
                print("Warning: API key not provided and GEMINI_API_KEY not found in environment variables. Translation may fail.")
                self.client = None
            else:
                self.client = genai.Client(api_key=api_key)

        self.max_concurrent_requests = max_concurrent_requests
        self._translation_cache = {}
        
    @property
    def is_available(self) -> bool:
        """Check if translation functionality is available."""
        return TRANSLATION_AVAILABLE
    
    def detect_arabic_text(self, text: str) -> bool:
        """
        Check if text contains Arabic characters.
        
        Args:
            text: Text to check
            
        Returns:
            True if text contains Arabic characters
        """
        if not isinstance(text, str):
            return False
        arabic_pattern = re.compile(r'[\u0600-\u06FF]')
        return bool(arabic_pattern.search(text))
    
    def get_arabic_indexes(self, strings: List[str]) -> List[int]:
        """
        Get indexes of strings containing Arabic characters.
        
        Args:
            strings: List of strings to check
            
        Returns:
            List of indexes where Arabic text was found
        """
        return [i for i, s in enumerate(strings) if self.detect_arabic_text(s)]
    
    async def translate_text(self, text: str, 
                           source_lang: str = 'ar', target_lang: str = 'en',
                           use_cache: bool = True) -> Optional[str]:
        """
        Translate a single text using Gemini API.
        
        Args:
            text: Text to translate
            source_lang: Source language code (default: 'ar')
            target_lang: Target language code (default: 'en')
            use_cache: Whether to use translation cache
            
        Returns:
            Translated text or None if translation failed
        """
        if not self.is_available or not self.client:
            return None
        
        # Check cache first
        if use_cache and text in self._translation_cache:
            return self._translation_cache[text]
        
        sys_prompt = """
        # Objective:
        You are an expert automotive translator specializing in converting Arabic vehicle brand and model names into their correct English equivalents. Your primary task is to transliterate Arabic text and match it to the official, internationally recognized vehicle names used by manufacturers.

        # Critical Requirements:
        1. **Accuracy Over Phonetics**: Always prioritize matching to actual vehicle models over phonetic similarity.
        2. **Use Official Names Only**: Return only the exact brand and model names as officially marketed by manufacturers.
        3. **Cross-Reference Known Models**: Verify your output against established automotive catalogs.
        4. **Handle Variations**: Arabic spellings may vary due to dialectical differences or transliteration inconsistencies.

        # Output Format:
        Return only the translated brand and model name in standard English format, with no additional commentary.
        """

        try:
            from google.genai import types

            response = await self.client.aio.models.generate_content(
                model="gemini-3-flash-preview",
                contents=[
                    sys_prompt,
                    f"Translate the following text from {source_lang} to {target_lang}: {text}"
                ],
                config=types.GenerateContentConfig(temperature=0.0)
            )
            translation = response.text.strip()
            
            # Cache the translation
            if use_cache:
                self._translation_cache[text] = translation
                
            return translation
            
        except Exception as e:
            print(f"Error translating text '{text}': {e}")
            return None
    
    async def translate_batch(self, texts: List[str],
                            progress_callback: Optional[Callable[[int, int], None]] = None,
                            source_lang: str = 'ar', target_lang: str = 'en',
                            use_cache: bool = True) -> Dict[str, str]:
        """
        Translate multiple texts with rate limiting and progress tracking.
        
        Args:
            texts: List of texts to translate
            progress_callback: Optional callback for progress updates
            source_lang: Source language code (default: 'ar')
            target_lang: Target language code (default: 'en')
            use_cache: Whether to use translation cache
            
        Returns:
            Dictionary mapping original texts to translations
        """
        if not self.is_available:
            return {}
        
        if not texts:
            return {}
        
        # Filter out duplicates and already cached items
        unique_texts = []
        translation_map = {}
        
        for text in texts:
            if use_cache and text in self._translation_cache:
                translation_map[text] = self._translation_cache[text]
            elif text not in unique_texts:
                unique_texts.append(text)
        
        if not unique_texts:
            return translation_map
        
        # Enable nested asyncio if needed
        nest_asyncio.apply()
        
        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def translate_with_semaphore(text: str, index: int) -> tuple:
            """Translate text with semaphore control."""
            async with semaphore:
                translation = await self.translate_text(
                    text, source_lang, target_lang, use_cache
                )
                if progress_callback:
                    progress_callback(index + 1, len(unique_texts))
                return text, translation
        
        # Execute translations
        if progress_callback:
            progress_callback(0, len(unique_texts))
        
        tasks = [
            translate_with_semaphore(text, i) 
            for i, text in enumerate(unique_texts)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Process results
        for original_text, translation in results:
            if translation:
                translation_map[original_text] = translation
            else:
                translation_map[original_text] = original_text  # Fallback to original
        
        return translation_map
    
    async def translate_dataframe_columns(self, df: pd.DataFrame, 
                                        columns: List[str],
                                        progress_callback: Optional[Callable[[int, int], None]] = None,
                                        use_cache: bool = True) -> pd.DataFrame:
        """
        Translate specified columns in a DataFrame containing Arabic text.
        
        Args:
            df: DataFrame to process
            columns: List of column names to translate
            progress_callback: Optional callback for progress updates
            use_cache: Whether to use translation cache
            
        Returns:
            DataFrame with translated columns
        """
        if not self.is_available:
            return df
        
        result_df = df.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            # Get unique values that need translation
            unique_values = df[col].astype(str).unique().tolist()
            arabic_values = [val for val in unique_values if self.detect_arabic_text(val)]
            
            if not arabic_values:
                continue
            
            # Translate the Arabic values
            translations = await self.translate_batch(
                arabic_values, progress_callback, use_cache=use_cache
            )
            
            # Apply translations to the column
            result_df[col] = result_df[col].astype(str).apply(
                lambda x: translations.get(x, x)
            )
        
        return result_df
    
    def clear_cache(self) -> None:
        """Clear the translation cache."""
        self._translation_cache.clear()
    
    def get_cache_size(self) -> int:
        """Get the number of cached translations."""
        return len(self._translation_cache)
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get translation cache statistics."""
        return {
            'cached_translations': len(self._translation_cache),
            'max_concurrent_requests': self.max_concurrent_requests
        }