"""
Gemini AI Verification Service

Provides AI-powered verification for fuzzy-matched vehicle makes using Google's Gemini API.
Validates that input and mapped makes actually refer to the same manufacturer.
"""

import asyncio
import nest_asyncio
import os
from typing import List, Tuple, Optional, Callable

# Gemini imports (optional)
try:
    from google import genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


class GeminiVerificationService:
    """
    Service for verifying fuzzy-matched vehicle makes using Gemini AI.

    Features:
    - Async batch verification with rate limiting
    - AI-powered validation of manufacturer matches
    - Progress tracking callbacks
    - Graceful error handling
    """

    def __init__(self, max_concurrent_requests: int = 100):
        """
        Initialize the Gemini verification service.

        Args:
            max_concurrent_requests: Maximum number of concurrent API requests
        """
        if not GEMINI_AVAILABLE:
            raise ImportError("google-genai package not installed. Install with: pip install google-genai")

        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")

        self.max_concurrent_requests = max_concurrent_requests
        self.client = genai.Client(api_key=api_key)

    @property
    def is_available(self) -> bool:
        """Check if Gemini functionality is available."""
        return GEMINI_AVAILABLE

    async def verify_make_mapping(self, input_make: str, mapped_make: str) -> bool:
        """
        Verify if input and mapped make refer to the same manufacturer.

        Args:
            input_make: Original make from input data
            mapped_make: Mapped make from reference data

        Returns:
            True if they refer to the same manufacturer, False otherwise
        """
        sys_prompt = '''String-level mapping has been performed. Given the input and mapped Vehicle Make below, determine if they refer to the same manufacturer.

Respond with only: True or False'''

        try:
            response = await self.client.aio.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=[
                    sys_prompt,
                    f'Input: {input_make}\nMapped: {mapped_make}'
                ]
            )
            result = response.text.strip().lower() == 'true'
            return result
        except Exception as e:
            # On failure, return False (treat as unmatched per user requirement)
            print(f"Gemini API error for {input_make} -> {mapped_make}: {str(e)}")
            return False

    async def verify_make_mappings_batch(
        self,
        mappings: List[Tuple[str, str, int]],
        progress_callback: Optional[Callable] = None
    ) -> List[Tuple[str, str, int, bool]]:
        """
        Verify multiple make mappings in parallel.

        Args:
            mappings: List of (input_make, mapped_make, score) tuples
            progress_callback: Optional callback function called after each verification

        Returns:
            List of (input_make, mapped_make, score, is_verified) tuples
        """
        nest_asyncio.apply()

        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def verify_with_semaphore(input_make, mapped_make, score):
            async with semaphore:
                verified = await self.verify_make_mapping(input_make, mapped_make)
                if progress_callback:
                    progress_callback()
                return (input_make, mapped_make, score, verified)

        tasks = [
            verify_with_semaphore(input_make, mapped_make, score)
            for input_make, mapped_make, score in mappings
        ]

        results = await asyncio.gather(*tasks)
        return results
