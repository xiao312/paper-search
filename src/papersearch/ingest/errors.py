from __future__ import annotations


class ProviderError(RuntimeError):
    pass


class ProviderRateLimited(ProviderError):
    pass


class ProviderUnauthorized(ProviderError):
    pass


class ProviderBadInput(ProviderError):
    pass
