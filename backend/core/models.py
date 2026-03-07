import uuid

from django.conf import settings
from django.db import models


class Dataset(models.Model):
    SOURCE_TYPE_CHOICES = [
        ("file", "File"),
        ("db", "Database"),
    ]
    FILE_FORMAT_CHOICES = [
        ("csv", "CSV"),
        ("xlsx", "XLSX"),
        ("json", "JSON"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="datasets",
    )
    name = models.CharField(max_length=200)
    source_type = models.CharField(max_length=20, choices=SOURCE_TYPE_CHOICES, default="file")
    original_filename = models.CharField(max_length=255, blank=True, null=True)
    file_format = models.CharField(max_length=10, choices=FILE_FORMAT_CHOICES, blank=True, null=True)
    import_settings = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [("owner", "name")]
        indexes = [
            models.Index(fields=["owner", "created_at"]),
        ]

    def __str__(self):
        return f"{self.name} ({self.owner})"


class Snapshot(models.Model):
    STAGE_CHOICES = [
        ("raw", "Raw"),
        ("cleaned", "Cleaned"),
        ("transformed", "Transformed"),
        ("ready", "Ready"),
    ]
    STORAGE_TYPE_CHOICES = [
        ("file", "File"),
        ("jsonb", "JSONB"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dataset = models.ForeignKey(
        Dataset,
        on_delete=models.CASCADE,
        related_name="snapshots",
    )
    parent_snapshot = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="children",
    )
    stage = models.CharField(max_length=30, choices=STAGE_CHOICES)
    is_active = models.BooleanField(default=False)
    is_ready_for_analysis = models.BooleanField(default=False)
    storage_type = models.CharField(max_length=20, choices=STORAGE_TYPE_CHOICES, default="file")
    data_path = models.TextField(blank=True, null=True)
    data_json = models.JSONField(blank=True, null=True)
    preview_rows = models.JSONField(blank=True, null=True)
    row_count = models.IntegerField(blank=True, null=True)
    column_count = models.IntegerField(blank=True, null=True)
    step_config = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["dataset", "created_at"]),
            models.Index(fields=["is_active"]),
            models.Index(fields=["is_ready_for_analysis"]),
        ]

    def __str__(self):
        return f"{self.dataset.name} / {self.stage} ({self.id})"


class ColumnMetadata(models.Model):
    INFERRED_TYPE_CHOICES = [
        ("int", "Integer"),
        ("float", "Float"),
        ("string", "String"),
        ("date", "Date"),
        ("bool", "Boolean"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    snapshot = models.ForeignKey(
        Snapshot,
        on_delete=models.CASCADE,
        related_name="column_metadata",
    )
    name = models.CharField(max_length=255)
    inferred_type = models.CharField(max_length=50, choices=INFERRED_TYPE_CHOICES)
    nullable = models.BooleanField(default=False)
    distinct_count = models.IntegerField(blank=True, null=True)
    missing_count = models.IntegerField(blank=True, null=True)
    stats = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("snapshot", "name")]
        indexes = [
            models.Index(fields=["snapshot"]),
            models.Index(fields=["name"]),
        ]

    def __str__(self):
        return f"{self.name} ({self.inferred_type}) — snapshot {self.snapshot_id}"


class Validation(models.Model):
    STATUS_CHOICES = [
        ("passed", "Passed"),
        ("failed", "Failed"),
        ("warn", "Warn"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    snapshot = models.ForeignKey(
        Snapshot,
        on_delete=models.CASCADE,
        related_name="validations",
    )
    rule_name = models.CharField(max_length=200)
    rule_params = models.JSONField(blank=True, null=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    failed_count = models.IntegerField(blank=True, null=True)
    sample_errors = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("snapshot", "rule_name")]
        indexes = [
            models.Index(fields=["snapshot"]),
            models.Index(fields=["status"]),
        ]

    def __str__(self):
        return f"{self.rule_name} [{self.status}] — snapshot {self.snapshot_id}"


class Dashboard(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="dashboards",
    )
    snapshot = models.ForeignKey(
        Snapshot,
        on_delete=models.CASCADE,
        related_name="dashboards",
    )
    title = models.CharField(max_length=200)
    description = models.TextField(blank=True, null=True)
    layout = models.JSONField(blank=True, null=True)
    global_filters = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["owner", "created_at"]),
            models.Index(fields=["snapshot"]),
        ]

    def __str__(self):
        return f"{self.title} ({self.owner})"


class Chart(models.Model):
    CHART_TYPE_CHOICES = [
        ("line", "Line"),
        ("bar", "Bar"),
        ("pie", "Pie"),
        ("table", "Table"),
        ("hist", "Histogram"),
    ]
    AGGREGATION_CHOICES = [
        ("sum", "Sum"),
        ("avg", "Average"),
        ("count", "Count"),
        ("min", "Min"),
        ("max", "Max"),
        ("median", "Median"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dashboard = models.ForeignKey(
        Dashboard,
        on_delete=models.CASCADE,
        related_name="charts",
    )
    chart_type = models.CharField(max_length=20, choices=CHART_TYPE_CHOICES)
    title = models.CharField(max_length=200, blank=True, null=True)
    x = models.CharField(max_length=255, blank=True, null=True)
    y = models.JSONField(blank=True, null=True)
    aggregation = models.CharField(max_length=30, choices=AGGREGATION_CHOICES, blank=True, null=True)
    group_by = models.JSONField(blank=True, null=True)
    filters = models.JSONField(blank=True, null=True)
    options = models.JSONField(blank=True, null=True)
    position = models.JSONField(blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=["dashboard"]),
            models.Index(fields=["chart_type"]),
        ]

    def __str__(self):
        return f"{self.chart_type} — {self.title or '(no title)'}"


class Experiment(models.Model):
    TEST_TYPE_CHOICES = [
        ("t_test", "T-test"),
        ("z_test", "Z-test"),
    ]
    STATUS_CHOICES = [
        ("draft", "Draft"),
        ("running", "Running"),
        ("done", "Done"),
        ("failed", "Failed"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="experiments",
    )
    snapshot = models.ForeignKey(
        Snapshot,
        on_delete=models.CASCADE,
        related_name="experiments",
    )
    name = models.CharField(max_length=200)
    hypothesis = models.TextField(blank=True, null=True)
    metric_column = models.CharField(max_length=255)
    group_column = models.CharField(max_length=255)
    alpha = models.DecimalField(max_digits=4, decimal_places=3, default="0.050")
    test_type = models.CharField(max_length=20, choices=TEST_TYPE_CHOICES)
    srm_enabled = models.BooleanField(default=False)
    cuped_enabled = models.BooleanField(default=False)
    cuped_covariate = models.CharField(max_length=255, blank=True, null=True)
    config = models.JSONField(blank=True, null=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="draft")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["owner", "status", "created_at"]),
            models.Index(fields=["snapshot"]),
        ]

    def __str__(self):
        return f"{self.name} [{self.status}] ({self.owner})"


class ExperimentResult(models.Model):
    DECISION_CHOICES = [
        ("significant", "Significant"),
        ("not_significant", "Not Significant"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    experiment = models.ForeignKey(
        Experiment,
        on_delete=models.CASCADE,
        related_name="results",
    )
    computed_at = models.DateTimeField(auto_now_add=True)
    n_control = models.IntegerField()
    n_variant = models.IntegerField()
    mean_control = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    mean_variant = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    effect_abs = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    effect_rel = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    p_value = models.DecimalField(max_digits=20, decimal_places=10, blank=True, null=True)
    ci_low = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    ci_high = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    srm_p_value = models.DecimalField(max_digits=20, decimal_places=10, blank=True, null=True)
    cuped_theta = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    decision = models.CharField(max_length=20, choices=DECISION_CHOICES)
    report_json = models.JSONField(blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=["experiment", "computed_at"]),
            models.Index(fields=["decision"]),
        ]

    def __str__(self):
        return f"Result [{self.decision}] for experiment {self.experiment_id}"
