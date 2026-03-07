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


# ── Stage 6: cleaning / transformation ──────────────────────────────


class DataCleanError(DataImportError):
    """Base exception for cleaning and transformation operations."""

    def __init__(self, message, code="clean_error", details=None):
        super().__init__(message, code=code, details=details)


class OperationConfigError(DataCleanError):
    """Invalid operation configuration supplied by the client."""

    def __init__(self, message, details=None):
        super().__init__(message, code="operation_config_error", details=details)


class ColumnNotFoundError(DataCleanError):
    """Referenced column does not exist in the DataFrame."""

    def __init__(self, message, details=None):
        super().__init__(message, code="column_not_found", details=details)


class TypeConversionError(DataCleanError):
    """Type cast could not be completed."""

    def __init__(self, message, details=None):
        super().__init__(message, code="type_conversion_error", details=details)


class EmptyResultError(DataCleanError):
    """Operation produced an empty DataFrame (0 rows)."""

    def __init__(self, message, details=None):
        super().__init__(message, code="empty_result_error", details=details)


class AggregationConfigError(DataCleanError):
    """Invalid aggregation configuration."""

    def __init__(self, message, details=None):
        super().__init__(message, code="aggregation_config_error", details=details)


# ── Stage 7: analytics / dashboards ─────────────────────────────────


class AnalyticsError(Exception):
    """Base exception for analytics and dashboard operations."""

    def __init__(self, message, code="analytics_error", details=None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class ChartConfigError(AnalyticsError):
    """Invalid chart configuration (columns, aggregation, filters)."""

    def __init__(self, message, details=None):
        super().__init__(message, code="chart_config_error", details=details)


class SnapshotLoadError(AnalyticsError):
    """Cannot load DataFrame from snapshot."""

    def __init__(self, message, details=None):
        super().__init__(message, code="snapshot_load_error", details=details)
