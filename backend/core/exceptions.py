class DataImportError(Exception):
    """Base import pipeline exception."""

    def __init__(self, message, code="import_error", details=None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class FileValidationError(DataImportError):
    """File-level checks (size, extension, missing file)."""

    def __init__(self, message, details=None):
        super().__init__(message, code="file_validation_error", details=details)


class FileParseError(DataImportError):
    """Pandas could not read the file (encoding, malformed, etc.)."""

    def __init__(self, message, details=None):
        super().__init__(message, code="file_parse_error", details=details)


class DataLimitError(DataImportError):
    """Row / column / size limits exceeded after reading."""

    def __init__(self, message, details=None):
        super().__init__(message, code="data_limit_error", details=details)


class DataValidationError(DataImportError):
    """Blocking validation rule failed (e.g. empty table)."""

    def __init__(self, message, details=None):
        super().__init__(message, code="data_validation_error", details=details)
