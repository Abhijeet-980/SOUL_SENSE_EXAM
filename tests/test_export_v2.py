"""
Comprehensive tests for enhanced export functionality (V2).

Tests cover:
- All export formats (JSON, CSV, XML, HTML, PDF)
- Date range filtering
- Data type selection
- Export encryption
- Export history tracking
- Error cases and edge cases
"""

import pytest
import json
import csv
import zipfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy.orm import Session

from backend.fastapi.api.services.export_service_v2 import ExportServiceV2
from backend.fastapi.api.root_models import (
    User, Score, JournalEntry, ExportRecord,
    PersonalProfile, UserSettings
)


@pytest.fixture
def export_dir(tmp_path):
    """Create a temporary export directory."""
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    return export_dir


@pytest.fixture
def test_user_with_data(db: Session):
    """Create a test user with sample data for exports."""
    # Create user
    user = User(
        username="export_test_user",
        password_hash="test_hash",
        created_at=datetime.utcnow().isoformat()
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    # Create some scores
    for i in range(5):
        score = Score(
            user_id=user.id,
            username=user.username,
            total_score=75 + i,
            sentiment_score=0.5 + (i * 0.1),
            timestamp=datetime.utcnow() - timedelta(days=i),
            is_rushed=False,
            is_inconsistent=False,
            detailed_age_group="25-34"
        )
        db.add(score)

    # Create journal entries
    for i in range(3):
        journal = JournalEntry(
            user_id=user.id,
            content=f"Test journal entry {i}",
            entry_date=datetime.utcnow() - timedelta(days=i),
            sentiment_score=0.6,
            is_deleted=False,
            stress_level=3,
            energy_level=4
        )
        db.add(journal)

    # Create settings
    settings = UserSettings(
        user_id=user.id,
        theme="dark",
        question_count=20,
        sound_enabled=True,
        notifications_enabled=True,
        language="en"
    )
    db.add(settings)

    # Create personal profile
    profile = PersonalProfile(
        user_id=user.id,
        occupation="Engineer",
        education="Bachelor",
        email="test@example.com"
    )
    db.add(profile)

    db.commit()
    db.refresh(user)

    return user


class TestExportFormats:
    """Test all export formats."""

    def test_json_export(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test JSON export format."""
        # Mock export directory
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {
            "data_types": ["scores", "journal", "settings"],
            "include_metadata": True
        }

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        assert Path(filepath).exists()
        assert filepath.endswith(".json")

        # Verify JSON content
        with open(filepath, 'r') as f:
            data = json.load(f)

        assert "_export_metadata" in data
        assert data["_export_metadata"]["format"] == "json"
        assert "scores" in data
        assert len(data["scores"]) == 5
        assert "journal" in data
        assert len(data["journal"]) == 3

    def test_csv_export(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test CSV export format (ZIP archive)."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores", "journal"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "csv", options
        )

        assert Path(filepath).exists()
        assert filepath.endswith(".csv")

        # Verify it's a ZIP file
        with zipfile.ZipFile(filepath, 'r') as zip_file:
            file_list = zip_file.namelist()
            assert "scores.csv" in file_list
            assert "journal.csv" in file_list
            assert "metadata.json" in file_list

            # Verify CSV content
            with zip_file.open("scores.csv") as csv_file:
                content = csv_file.read().decode('utf-8-sig')
                reader = csv.DictReader(io.StringIO(content))
                rows = list(reader)
                assert len(rows) == 5

    def test_xml_export(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test XML export format."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "xml", options
        )

        assert Path(filepath).exists()
        assert filepath.endswith(".xml")

        # Verify XML structure
        tree = ET.parse(filepath)
        root = tree.getroot()

        assert root.tag == "SoulSenseExport"
        assert root.find("ExportMetadata") is not None
        assert root.find("scores") is not None

    def test_html_export(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test HTML export format."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores", "profile"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "html", options
        )

        assert Path(filepath).exists()
        assert filepath.endswith(".html")

        # Verify HTML content
        with open(filepath, 'r') as f:
            content = f.read()

        assert "<!DOCTYPE html>" in content
        assert "Soul Sense Data Export" in content
        assert "search-box" in content
        assert "scores" in content or "Scores" in content

    def test_pdf_export(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test PDF export format."""
        pytest.importorskip("reportlab")

        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "pdf", options
        )

        assert Path(filepath).exists()
        assert filepath.endswith(".pdf")

        # Verify it's a valid PDF (starts with %PDF)
        with open(filepath, 'rb') as f:
            header = f.read(4)
            assert header == b"%PDF"


class TestDateRangeFiltering:
    """Test date range filtering functionality."""

    def test_export_with_start_date(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test export with start date filter."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        start_date = (datetime.utcnow() - timedelta(days=2)).isoformat()

        options = {
            "data_types": ["scores"],
            "date_range": {"start": start_date}
        }

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        with open(filepath, 'r') as f:
            data = json.load(f)

        # Should only include scores from last 2 days
        assert len(data["scores"]) <= 3  # scores[0], scores[1], scores[2]

    def test_export_with_end_date(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test export with end date filter."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        end_date = (datetime.utcnow() - timedelta(days=3)).isoformat()

        options = {
            "data_types": ["scores"],
            "date_range": {"end": end_date}
        }

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        with open(filepath, 'r') as f:
            data = json.load(f)

        # Should only include older scores
        assert len(data["scores"]) <= 3

    def test_export_with_both_dates(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test export with both start and end date."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        start_date = (datetime.utcnow() - timedelta(days=4)).isoformat()
        end_date = (datetime.utcnow() - timedelta(days=2)).isoformat()

        options = {
            "data_types": ["scores"],
            "date_range": {"start": start_date, "end": end_date}
        }

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        with open(filepath, 'r') as f:
            data = json.load(f)

        # Should only include scores in the date range
        assert len(data["scores"]) <= 3


class TestDataTypeSelection:
    """Test granular data type selection."""

    def test_export_single_data_type(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test exporting only one data type."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        with open(filepath, 'r') as f:
            data = json.load(f)

        assert "scores" in data
        assert "journal" not in data
        assert "settings" not in data

    def test_export_multiple_data_types(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test exporting multiple data types."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores", "journal", "settings"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        with open(filepath, 'r') as f:
            data = json.load(f)

        assert "scores" in data
        assert "journal" in data
        assert "settings" in data
        assert "profile" not in data


class TestExportHistory:
    """Test export history tracking."""

    def test_export_recorded_in_database(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test that exports are recorded in database."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        # Verify database record
        export_record = db.query(ExportRecord).filter(
            ExportRecord.export_id == export_id
        ).first()

        assert export_record is not None
        assert export_record.user_id == test_user_with_data.id
        assert export_record.format == "json"
        assert export_record.status == "completed"

    def test_get_export_history(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test retrieving export history."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        # Create multiple exports
        for format in ["json", "csv", "html"]:
            options = {"data_types": ["scores"]}
            ExportServiceV2.generate_export(db, test_user_with_data, format, options)

        # Get history
        history = ExportServiceV2.get_export_history(db, test_user_with_data)

        assert len(history) == 3
        formats = [e["format"] for e in history]
        assert "json" in formats
        assert "csv" in formats
        assert "html" in formats

    def test_delete_export(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test deleting an export."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}
        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        # Verify file exists
        assert Path(filepath).exists()

        # Delete export
        success = ExportServiceV2.delete_export(db, test_user_with_data, export_id)

        assert success is True
        assert not Path(filepath).exists()

        # Verify database record is deleted
        export_record = db.query(ExportRecord).filter(
            ExportRecord.export_id == export_id
        ).first()
        assert export_record is None


class TestErrorHandling:
    """Test error cases and validation."""

    def test_invalid_format_raises_error(self, db: Session, test_user_with_data):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported format"):
            ExportServiceV2.generate_export(
                db, test_user_with_data, "invalid_format", {}
            )

    def test_invalid_data_types_raises_error(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test that invalid data types raise ValueError."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["invalid_type"]}

        with pytest.raises(ValueError, match="Invalid data types"):
            ExportServiceV2.generate_export(
                db, test_user_with_data, "json", options
            )

    def test_encryption_without_password_raises_error(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test that encryption without password raises ValueError."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {
            "data_types": ["scores"],
            "encrypt": True
        }

        with pytest.raises(ValueError, match="Password is required"):
            ExportServiceV2.generate_export(
                db, test_user_with_data, "json", options
            )


class TestGDPRCompliance:
    """Test GDPR compliance features."""

    def test_export_metadata_includes_required_fields(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test that export metadata includes GDPR-required fields."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        with open(filepath, 'r') as f:
            data = json.load(f)

        metadata = data["_export_metadata"]

        # Required GDPR fields
        assert "version" in metadata
        assert "exported_at" in metadata
        assert "export_id" in metadata
        assert "user_id" in metadata
        assert "data_controller" in metadata
        assert "purpose" in metadata
        assert "schema" in metadata

    def test_export_includes_data_lineage(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test that export includes data lineage information."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}

        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        with open(filepath, 'r') as f:
            data = json.load(f)

        metadata = data["_export_metadata"]

        assert "data_lineage" in metadata
        assert "sources" in metadata["data_lineage"]
        assert "processing_history" in metadata["data_lineage"]


class TestCsvSanitization:
    """Test CSV sanitization to prevent injection attacks."""

    def test_csv_sanitization(self, db: Session, export_dir, monkeypatch):
        """Test that CSV fields are sanitized to prevent formula injection."""
        from backend.fastapi.api.services.export_service_v2 import ExportServiceV2

        # Test various injection attempts
        test_cases = [
            ("=1+1", "'=1+1"),
            ("+1+1", "'+1+1"),
            ("-1+1", "'-1+1"),
            ("@SUM(A1:A10)", "'@SUM(A1:A10)"),
            ("normal text", "normal text"),
            (None, ""),
            ("", ""),
            (123, "123"),
        ]

        for input_val, expected in test_cases:
            result = ExportServiceV2._sanitize_csv_field(input_val)
            assert result == expected, f"Failed for input: {input_val}"


class TestExportExpiration:
    """Test export expiration and cleanup."""

    def test_export_has_expiration_date(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test that exports have expiration dates set."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        options = {"data_types": ["scores"]}
        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        export_record = db.query(ExportRecord).filter(
            ExportRecord.export_id == export_id
        ).first()

        assert export_record.expires_at is not None
        assert export_record.expires_at > datetime.utcnow()

    def test_cleanup_old_exports(self, db: Session, test_user_with_data, export_dir, monkeypatch):
        """Test cleanup of expired exports."""
        monkeypatch.setattr(ExportServiceV2, "EXPORT_DIR", export_dir)

        # Create an export
        options = {"data_types": ["scores"]}
        filepath, export_id = ExportServiceV2.generate_export(
            db, test_user_with_data, "json", options
        )

        # Manually set expiration to past
        export_record = db.query(ExportRecord).filter(
            ExportRecord.export_id == export_id
        ).first()
        export_record.expires_at = datetime.utcnow() - timedelta(hours=1)
        db.commit()

        # Run cleanup
        ExportServiceV2.cleanup_old_exports(db, max_age_hours=0)

        # Verify export is marked as expired
        db.refresh(export_record)
        assert export_record.status == "expired"
