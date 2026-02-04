"""
OpenRouter LLM client for Synapse.

Handles all LLM calls with timeouts and cost optimization:
- Rolling summary compression
- Episode summary generation
- Loop extraction (when regex gate triggers)
"""

import asyncio
import httpx
import json
import logging
from typing import Optional, Dict, Any, List, Literal
from .config import get_settings

logger = logging.getLogger(__name__)


class OpenRouterClient:
    def __init__(self):
        self.settings = get_settings()
        self.api_key = getattr(self.settings, 'openrouter_api_key', None) or self.settings.openai_api_key
        self.model = getattr(self.settings, 'openrouter_model', 'anthropic/claude-3.5-haiku')
        self.model_summary = getattr(self.settings, 'openrouter_model_summary', 'amazon/nova-micro')
        self.model_loops = getattr(self.settings, 'openrouter_model_loops', 'xiaomi/mimo-v2-flash')
        self.model_session_episode = getattr(self.settings, 'openrouter_model_session_episode', 'xiaomi/mimo-v2-flash')
        self.model_fallback = getattr(self.settings, 'openrouter_model_fallback', 'mistral/ministral-3b')
        self.reasoning_enabled = bool(getattr(self.settings, 'openrouter_reasoning_enabled', False))
        self.timeout = float(getattr(self.settings, 'llm_timeout', 10))
        self.base_url = "https://openrouter.ai/api/v1"

    async def generate_rolling_summary(
        self,
        messages_to_compress: List[Dict[str, str]],
        existing_summary: Optional[str] = None
    ) -> Optional[str]:
        """
        Compress older conversation turns into a brief summary.

        Args:
            messages_to_compress: List of {role, text} dicts to summarize
            existing_summary: Previous rolling summary to build upon

        Returns:
            1-2 sentence summary, or None if LLM call fails
        """
        try:
            # Build context
            turns = "\n".join([
                f"{msg['role']}: {msg['text']}"
                for msg in messages_to_compress
            ])

            prompt = f"""Compress the following conversation turns into a brief 1-2 sentence summary.
Focus on key topics, decisions, or context that would be useful for future conversation.

{f"Previous summary: {existing_summary}" if existing_summary else ""}

Conversation to compress:
{turns}

Summary:"""

            response = await self._call_llm(
                prompt=prompt,
                max_tokens=100,
                temperature=0.3,
                task="summary"
            )

            if response:
                summary = response.strip()
                logger.info(f"Generated rolling summary: {summary[:50]}...")
                return summary

            return None

        except Exception as e:
            logger.error(f"Failed to generate rolling summary: {e}")
            return None

    async def generate_episode_summary(
        self,
        rolling_summary: Optional[str],
        recent_messages: List[Dict[str, str]]
    ) -> Optional[str]:
        """
        Generate an episode summary when closing a session.

        Args:
            rolling_summary: Compressed history
            recent_messages: Last 6 messages

        Returns:
            2-3 sentence episode summary for Graphiti
        """
        try:
            recent_turns = "\n".join([
                f"{msg['role']}: {msg['text']}"
                for msg in recent_messages
            ])

            prompt = f"""Create a 2-3 sentence summary of this conversation session.
Capture the main topics discussed, any decisions made, and emotional tone.
This will be stored as an episode in long-term memory.

{f"Earlier in session: {rolling_summary}" if rolling_summary else ""}

Recent conversation:
{recent_turns}

Episode summary:"""

            response = await self._call_llm(
                prompt=prompt,
                max_tokens=150,
                temperature=0.3,
                task="session_episode"
            )

            if response:
                episode = response.strip()
                logger.info(f"Generated episode summary: {episode[:50]}...")
                return episode

            return None

        except Exception as e:
            logger.error(f"Failed to generate episode summary: {e}")
            return None

    async def extract_loop(self, message: str) -> Optional[Dict[str, Any]]:
        """
        Extract structured loop data from a message (after regex gate passes).

        Args:
            message: User message containing commitment signals

        Returns:
            Dict with {type, text, deadline, priority} or None
        """
        try:
            prompt = f"""Analyze this message and extract any commitment, habit, or task.

Message: "{message}"

If there's a commitment/task/reminder, respond with JSON:
{{
  "type": "commitment|habit|friction|thread",
  "text": "natural language description",
  "deadline": "YYYY-MM-DD or 'none'",
  "priority": "high|medium|low"
}}

If there's no clear commitment, respond with: {{"type": "none"}}

JSON:"""

            response = await self._call_llm(
                prompt=prompt,
                max_tokens=200,
                temperature=0.1,
                task="loops"
            )

            if response:
                # Parse JSON response
                try:
                    loop_data = json.loads(response.strip())
                    if loop_data.get("type") != "none":
                        logger.info(f"Extracted loop: {loop_data['type']}")
                        return loop_data
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse loop JSON: {response}")

            return None

        except Exception as e:
            logger.error(f"Failed to extract loop: {e}")
            return None

    async def _call_llm(
        self,
        prompt: str,
        max_tokens: int = 500,
        temperature: float = 0.7,
        task: Literal["summary", "loops", "session_episode", "generic"] = "generic"
    ) -> Optional[str]:
        """
        Make a request to OpenRouter API with timeout.

        Returns:
            Response text or None if call fails/times out
        """
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            model = self.model_fallback
            if task == "summary":
                model = self.model_summary
            elif task == "loops":
                model = self.model_loops
            elif task == "session_episode":
                model = self.model_session_episode
            elif task == "generic":
                model = self.model

            payload = {
                "model": model,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": max_tokens,
                "temperature": temperature
            }
            if self.reasoning_enabled:
                payload["reasoning"] = {"enabled": True}
            if task == "loops":
                payload["response_format"] = {"type": "json_object"}

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=payload
                )

                if response.status_code == 200:
                    data = response.json()
                    return data["choices"][0]["message"]["content"]
                else:
                    logger.error(f"OpenRouter API error: {response.status_code} - {response.text}")
                    return None

        except asyncio.TimeoutError:
            logger.warning(f"LLM call timed out after {self.timeout}s")
            return None
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return None


# Module-level instance
_client: Optional[OpenRouterClient] = None


def get_llm_client() -> OpenRouterClient:
    """Get or create the global LLM client"""
    global _client
    if _client is None:
        _client = OpenRouterClient()
    return _client
