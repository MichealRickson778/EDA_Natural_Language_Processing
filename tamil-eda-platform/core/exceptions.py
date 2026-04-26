class PlatformError(Exception):
    """
    Base Exception for the entire platform

    EVery other exception inherits from this.
    Catching PlatformError catches everything.
    """

    def __init__(self, message: str, details : dict | None=None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | details = {self.details}"
        return self.message

#PIPELINE-------------------------------------------------------------

class PipelineError(PlatformError):
        """ Any faliure inside the DAG """

class IngestionError(PipelineError):
        """ File read or parse failure inside the ingestor"""

class ProcessingError(PipelineError):
        """ Preprocessor, normalizer, or formatter faliure"""

class TranslationError(PipelineError):
        """Translation API call failed or returned unsuable output"""

class ValidationError(PipelineError):
      """ Schema check, quality gate, or dublicate check failed"""

#Storage----------------------------------------------------------------

class StorageError(PlatformError):
      """Any faliure inside the storage layer"""

class VersionError(StorageError):
      """Version manager conflict - e.g version already exists"""

#Security---------------------------------------------------------------

class SecurityError(PlatformError):
      """Base class for all security-related faliures"""

class SanitizationError(SecurityError):
      """Prompt injection or forbidden content detected in input"""

class EncryptionError(SecurityError):
      """AES encryption or decryption failed"""

class SecretsError(SecurityError):
      """Vault unreachable, key missing or rotation failed"""

#Infrostructure---------------------------------------------------------

class ConfigrationError(PlatformError):
      """Bad yaml, missing required key, or invalid config value"""

class QueueError(PlatformError):
      """Queue timeout, full, or connection faliure"""

class DAGError(PlatformError):
      """Cycle detected, missing stage, or invalid DAG definition"""

class ConnectionError(PlatformError):
      """DB,Api,file, or cloud connector faliure"""

#Language----------------------------------------------------------------

class LanguageError(PlatformError):
      """Base class for language detection and NLP faliure"""

class DetectionError(LanguageError):
      """Language detection returned low confidance or failed entirely"""

class TokenizationError(LanguageError):
      """Tokenizer failed to spilt text into sentence or words"""

# API-------------------------------------------------------------------

class RateLimitError(PlatformError):
      """Raise when rate limit exceed"""

# Event-----------------------------------------------------------------
class EventCreationError(PlatformError):
    """Raised when event schema validation fails"""


# DAG -------------------------------

class DAGBuildError(PlatformError):
    """Raised when DAG assembly fails"""

class DAGValidationError(PlatformError):
    """Raised when DAG validation fails"""

class DAGExecutionError(PlatformError):
    """Raised when DAG execution fails"""

class StageTimeoutError(PlatformError):
    """Raised when a stage exceeds its timeout"""

class StageFailureError(PlatformError):
    """Raised when a required stage fails"""

class StageOutputError(PlatformError):
    """Raised when a required stage returns None"""
