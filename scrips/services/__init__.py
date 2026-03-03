"""
Services module for vehicle specification analysis.

This module contains service classes that provide specialized functionality
for vehicle data processing, including translation, validation, and analysis.
"""

from .translation_service import ArabicTranslationService
from .gemini_verification_service import GeminiVerificationService
from .gcc_presence_service import GCCPresenceService, GEMINI_AVAILABLE

__all__ = ['ArabicTranslationService', 'GeminiVerificationService', 'GCCPresenceService', 'GEMINI_AVAILABLE']