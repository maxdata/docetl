from motion.operators.base_operator import Operator
from motion.llm_profiler import LLMCallTracker
from typing import List, Any, Tuple, Set, Optional, Dict
from abc import ABC, abstractmethod
from motion.types import RK, RV, K, V


class KeyResolver(Operator, ABC):
    compute_embeddings: bool = False

    def precheck(self, x: K, y: K) -> bool:
        return True

    @abstractmethod
    def get_label_key(self, keys: Set[K]) -> K:
        pass

    def get_embedding(self, key: K) -> Optional[List[float]]:
        if self.compute_embeddings:
            raise NotImplementedError(
                "Embedding computation is not implemented. Set compute_embeddings to True if you want to use this method."
            )
        return None

    def validate(self, input_key: K, output_key: K) -> bool:
        return True

    def correct(self, input_key: K, output_key: K) -> K:
        return output_key


class PairwiseKeyResolver(KeyResolver, ABC):
    @abstractmethod
    def are_equal(self, x: K, y: K) -> bool:
        pass


class LLMPairwiseKeyResolver(PairwiseKeyResolver):
    def __init__(self, model: str, **llm_kwargs):
        self.tracker = LLMCallTracker()
        self.model = model
        self.llm_kwargs = llm_kwargs

    def generate_prompt(self, x: K, y: K) -> list:
        return [
            {
                "role": "system",
                "content": "You are a key resolver. Your task is to determine if two keys are equal.",
            },
            {
                "role": "user",
                "content": f"Are these two keys equal? Key 1: {x}, Key 2: {y}. Respond with 'Yes' or 'No'.",
            },
        ]

    def process_response(self, response: Any, **prompt_kwargs) -> bool:
        return response.choices[0].message.content.strip().lower() == "yes"

    def are_equal(self, x: K, y: K) -> bool:
        with self.tracker.track_call():
            prompt = self.generate_prompt(x, y)
            response = self.tracker.completion(
                messages=prompt, model=self.model, **self.llm_kwargs
            )
            return self.process_response(response, x=x, y=y)

    def get_label_key(self, keys: Set[K]) -> K:
        return next(iter(keys))

    def execute(self, x: K, y: K) -> Tuple[bool, Dict]:
        result = self.are_equal(x, y)
        return result, {
            "prompt": self.tracker.last_prompt,
            "response": self.tracker.last_response,
        }

    def validate(self, input_key: K, output_key: K) -> bool:
        return True

    def correct(self, input_key: K, output_key: K) -> K:
        return output_key


class ListKeyResolver(KeyResolver, ABC):
    @abstractmethod
    def assign_key(self, key: K, label_keys: List[K]) -> K:
        pass


class LLMListKeyResolver(ListKeyResolver):
    def __init__(self, model: str, **llm_kwargs):
        self.tracker = LLMCallTracker()
        self.model = model
        self.llm_kwargs = llm_kwargs

    def generate_prompt(self, key: K, label_keys: List[K]) -> list:
        return [
            {
                "role": "system",
                "content": "You are a key resolver. Your task is to assign a key to a group based on existing label keys.",
            },
            {
                "role": "user",
                "content": f"Given the key '{key}' and the existing label keys {label_keys}, which label key should it be assigned to? If it doesn't match any existing label keys, respond with 'NEW'. Provide your answer as a single word or 'NEW'.",
            },
        ]

    def process_response(self, response: Any, **prompt_kwargs) -> K:
        content = response.choices[0].message.content.strip()
        return content if content != "NEW" else prompt_kwargs["key"]

    def assign_key(self, key: K, label_keys: List[K]) -> K:
        with self.tracker.track_call():
            prompt = self.generate_prompt(key, label_keys)
            response = self.tracker.completion(
                messages=prompt, model=self.model, **self.llm_kwargs
            )
            return self.process_response(response, key=key, label_keys=label_keys)

    def execute(self, key: K, label_keys: List[K]) -> Tuple[K, Dict]:
        result = self.assign_key(key, label_keys)
        return result, {
            "prompt": self.tracker.last_prompt,
            "response": self.tracker.last_response,
        }

    def validate(self, input_key: K, output_key: K) -> bool:
        return True

    def correct(self, input_key: K, output_key: K) -> K:
        return output_key