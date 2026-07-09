from django.db import models


class LogEntry(models.Model):
    """
    Maps to the `logs` table created by db-init/001_create_logs_table.sql.
    managed = False tells Django: never create/alter/drop this table via
    migrations. The Go consumer's storage_writer owns writes; this model
    is purely a read/query interface for the admin dashboard.
    """

    id = models.BigAutoField(primary_key=True)
    service = models.CharField(max_length=100)
    severity = models.CharField(max_length=20)
    message = models.TextField()
    routing_key = models.CharField(max_length=150)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "logs"
        ordering = ["-created_at"]
        verbose_name = "Log Entry"
        verbose_name_plural = "Log Entries"

    def __str__(self):
        return f"[{self.routing_key}] {self.message[:50]}"