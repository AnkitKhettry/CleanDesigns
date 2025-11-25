from abc import ABC, abstractmethod

class RateLimiter(ABC):
    @abstractmethod
    def allow_request(self, client_id: str, cost: int = 1) -> bool:
        return True
