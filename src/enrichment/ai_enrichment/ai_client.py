import json
import time
from typing import Optional

from openai import OpenAI
from openai import APIError, RateLimitError

from config.logger_config import logger


class MockAIClient:
    def __init__(self, mode="normal"):
        self.mode = mode

    @staticmethod
    def generate(self, prompt: str) -> Optional[str]:
        if "technology" in prompt.lower():
            segment = "high_fit"
        else:
            segment = "medium_fit"
        try:
            response = f"""
            {
                "industry_ai": "technology",
                "confidence": 0.85,
                "short_description": "Provides software solutions",
                "segment": {segment},
                "sales_relevance": 0.8,
                "buying_signal": 1,
                "source": "mock_ai"
            }
            """

            if self.mode == "timeout":
                raise TimeoutError("Simulated timeout")

            return json.loads(response)

        except Exception as e:
            logger.exception(f"AI call failed {e}")
            return f""" {
                "error": str{e},
                "source": "mock_ai"
            }"""


class AIClient:
    """
    Client for interacting with OpenAI API.

    Handles:
    - API initialization
    - request execution
    - retries
    - logging
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        max_retries: int = 3,
        timeout: int = 30,
    ):
        if not api_key:
            raise ValueError("API key must be provided")

        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.max_retries = max_retries
        self.timeout = timeout

    def generate(self, prompt: str) -> Optional[str]:
        """
        Send prompt to OpenAI and return response content.
        """

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(
                    "AI request attempt",
                    extra={"attempt": attempt, "model": self.model}
                )

                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a precise data enrichment assistant."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.2,
                    timeout=self.timeout,
                )

                content = response.choices[0].message.content

                if not content:
                    logger.warning("Empty AI response")
                    return None

                return content

            except RateLimitError as e:
                logger.warning(f"Rate limit hit (attempt {attempt}): {e}")
                time.sleep(2 ** attempt)

            except APIError as e:
                logger.warning(f"OpenAI API error (attempt {attempt}): {e}")
                time.sleep(2 ** attempt)

            except Exception as e:
                logger.exception(f"Unexpected AI client error: {e}")
                break

        logger.error("AI request failed after max retries")
        return None
