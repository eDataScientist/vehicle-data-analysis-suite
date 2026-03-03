"""
Gemini AI Verification Service

Provides AI-powered verification for fuzzy-matched vehicle makes using Google's Gemini API.
Validates that input and mapped makes actually refer to the same manufacturer.
"""

import asyncio
import nest_asyncio
import os
import json
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

    async def verify_make_mapping(self, input_make: str, mapped_make: str) -> Tuple[bool, str]:
        """
        Verify if input and mapped make refer to the same manufacturer.

        Args:
            input_make: Original make from input data
            mapped_make: Mapped make from reference data

        Returns:
            Tuple of (is_verified, reasoning)
        """
        sys_prompt = '''String-level mapping has been performed. Given the input and mapped Vehicle Make below, determine if they refer to the same manufacturer.

Respond in the following JSON format:
{"verdict": true/false, "reason": "brief explanation of why they match or don't match"}

Respond with ONLY the JSON object, no other text.'''

        try:
            response = await self.client.aio.models.generate_content(
                model="gemini-3-flash-preview",
                contents=[
                    sys_prompt,
                    f'Input: {input_make}\nMapped: {mapped_make}'
                ]
            )
            raw = response.text.strip()
            # Strip markdown code fences if present
            if raw.startswith('```'):
                raw = raw.split('\n', 1)[-1].rsplit('```', 1)[0].strip()

            parsed = json.loads(raw)
            verdict = bool(parsed.get('verdict', False))
            reason = parsed.get('reason', 'No reason provided')
            return (verdict, reason)
        except (json.JSONDecodeError, KeyError):
            # Fallback: try simple True/False parsing
            text = response.text.strip().lower()
            verdict = text.startswith('true') or '"verdict": true' in text
            return (verdict, f'Raw response (parse failed): {response.text.strip()}')
        except Exception as e:
            print(f"Gemini API error for {input_make} -> {mapped_make}: {str(e)}")
            return (False, f'API error: {str(e)}')

    async def verify_make_mappings_batch(
        self,
        mappings: List[Tuple[str, str, int]],
        progress_callback: Optional[Callable] = None
    ) -> List[Tuple[str, str, int, bool, str]]:
        """
        Verify multiple make mappings in parallel.

        Args:
            mappings: List of (input_make, mapped_make, score) tuples
            progress_callback: Optional callback function called after each verification

        Returns:
            List of (input_make, mapped_make, score, is_verified, reasoning) tuples
        """
        nest_asyncio.apply()

        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def verify_with_semaphore(input_make, mapped_make, score):
            async with semaphore:
                verified, reason = await self.verify_make_mapping(input_make, mapped_make)
                if progress_callback:
                    progress_callback()
                return (input_make, mapped_make, score, verified, reason)

        tasks = [
            verify_with_semaphore(input_make, mapped_make, score)
            for input_make, mapped_make, score in mappings
        ]

        results = await asyncio.gather(*tasks)
        return results
