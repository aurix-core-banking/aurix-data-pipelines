from datetime import datetime, timedelta

import pytest

from data_compliance import DataClassification, DataRetention, DataSubject, LGPDCompliance


@pytest.fixture
def lgpd():
    return LGPDCompliance({})


class TestClassifyData:
    def test_classify_public_if_no_sensitive_data(self, lgpd):
        data = {"name": "Joao", "age": "30"}
        classification = lgpd.classify_data(data)
        assert classification == DataClassification.PUBLIC

    def test_classify_internal_with_one_sensitive_field(self, lgpd):
        data = {"name": "Joao", "cpf": "123.456.789-00"}
        classification = lgpd.classify_data(data)
        assert classification == DataClassification.INTERNAL

    def test_classify_confidential_with_two_sensitive_fields(self, lgpd):
        data = {
            "name": "Joao",
            "cpf": "123.456.789-00",
            "email": "joao@example.com",
        }
        classification = lgpd.classify_data(data)
        assert classification == DataClassification.CONFIDENTIAL

    def test_classify_restricted_with_three_or_more_sensitive_fields(self, lgpd):
        data = {
            "name": "Joao",
            "cpf": "123.456.789-00",
            "email": "joao@example.com",
            "phone": "(11) 91234-5678",
        }
        classification = lgpd.classify_data(data)
        assert classification == DataClassification.RESTRICTED

    def test_classify_cnpj_detected_as_sensitive(self, lgpd):
        data = {"cnpj": "12.345.678/0001-90"}
        classification = lgpd.classify_data(data)
        assert classification == DataClassification.INTERNAL

    def test_classify_credit_card_detected_as_sensitive(self, lgpd):
        data = {"card": "1234 5678 9012 3456"}
        classification = lgpd.classify_data(data)
        assert classification == DataClassification.INTERNAL


class TestAnonymizeData:
    def test_anonymize_cpf(self, lgpd):
        data = {"cpf": "12345678901"}
        result = lgpd.anonymize_data(data, ["cpf"])
        assert result["cpf"] == "***.***.***-01"

    def test_anonymize_email(self, lgpd):
        data = {"email": "joao.silva@example.com"}
        result = lgpd.anonymize_data(data, ["email"])
        assert result["email"] == "jo***@example.com"

    def test_anonymize_phone(self, lgpd):
        data = {"phone": "11912345678"}
        result = lgpd.anonymize_data(data, ["phone"])
        assert result["phone"] == "(11) ***-5678"

    def test_anonymize_name(self, lgpd):
        data = {"name": "Joao Silva"}
        result = lgpd.anonymize_data(data, ["name"])
        assert result["name"] == "Joao S***"

    def test_anonymize_single_name(self, lgpd):
        data = {"name": "Joao"}
        result = lgpd.anonymize_data(data, ["name"])
        assert result["name"] == "J***"

    def test_anonymize_unknown_field_uses_hash(self, lgpd):
        data = {"other_field": "some_value"}
        result = lgpd.anonymize_data(data, ["other_field"])
        assert len(result["other_field"]) == 8

    def test_anonymize_original_data_not_mutated(self, lgpd):
        original = {"cpf": "12345678901"}
        lgpd.anonymize_data(original, ["cpf"])
        assert original["cpf"] == "12345678901"


class TestCreateDataSubject:
    def test_create_data_subject_with_public_classification(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        subject = lgpd.create_data_subject(data)
        assert isinstance(subject, DataSubject)
        assert subject.data_classification == DataClassification.INTERNAL
        assert subject.data_retention == DataRetention.SHORT_TERM

    def test_create_data_subject_with_confidential_classification(self, lgpd):
        data = {
            "name": "Joao",
            "cpf": "123.456.789-00",
            "email": "joao@example.com",
        }
        subject = lgpd.create_data_subject(data)
        assert subject.data_classification == DataClassification.CONFIDENTIAL
        assert subject.data_retention == DataRetention.MEDIUM_TERM

    def test_create_data_subject_with_restricted_classification(self, lgpd):
        data = {
            "name": "Joao",
            "cpf": "123.456.789-00",
            "email": "joao@example.com",
            "phone": "(11) 91234-5678",
        }
        subject = lgpd.create_data_subject(data)
        assert subject.data_classification == DataClassification.RESTRICTED
        assert subject.data_retention == DataRetention.LONG_TERM

    def test_create_data_subject_has_id(self, lgpd):
        data = {"cpf": "123.456.789-00", "email": "joao@example.com"}
        subject = lgpd.create_data_subject(data)
        assert subject.id is not None
        assert len(subject.id) > 0

    def test_create_data_subject_stored_internal(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        subject = lgpd.create_data_subject(data)
        assert lgpd.data_subjects[subject.id] is subject


class TestRecordDataProcessing:
    def test_record_data_processing_creates_record(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        subject = lgpd.create_data_subject(data)
        record = lgpd.record_data_processing(
            data_subject_id=subject.id,
            processing_purpose="Analise de credito",
            legal_basis="consent",
            data_categories=["financeira", "pessoal"],
            processor="AUREUS System",
        )
        assert record.data_subject_id == subject.id
        assert record.processing_purpose == "Analise de credito"
        assert record.legal_basis == "consent"
        assert record.data_categories == ["financeira", "pessoal"]
        assert record.processor == "AUREUS System"

    def test_record_data_processing_raises_for_unknown_subject(self, lgpd):
        with pytest.raises(ValueError, match="Titular de dados nao encontrado"):
            lgpd.record_data_processing(
                data_subject_id="nonexistent",
                processing_purpose="test",
                legal_basis="consent",
                data_categories=[],
                processor="test",
            )

    def test_record_data_processing_sets_anonymization_for_confidential(self, lgpd):
        data = {
            "name": "Joao",
            "cpf": "123.456.789-00",
            "email": "joao@example.com",
        }
        subject = lgpd.create_data_subject(data)
        record = lgpd.record_data_processing(
            subject.id, "Marketing", "consent", ["pessoal"], "System"
        )
        assert record.anonymization_required is True


class TestCheckDataRetention:
    def test_retention_no_expired_records(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        subject = lgpd.create_data_subject(data)
        lgpd.record_data_processing(
            subject.id, "Analise", "consent", ["pessoal"], "System"
        )
        expired = lgpd.check_data_retention()
        assert expired == []

    def test_retention_detects_expired_records(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        subject = lgpd.create_data_subject(data)
        lgpd.record_data_processing(
            subject.id, "Analise", "consent", ["pessoal"], "System"
        )
        lgpd.processing_records[0].processing_date = datetime.now() - timedelta(
            days=400
        )
        expired = lgpd.check_data_retention()
        assert len(expired) == 1
        assert expired[0]["data_subject_id"] == subject.id

    def test_retention_skips_permanent_records(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        subject = lgpd.create_data_subject(data)
        subject.data_retention = DataRetention.PERMANENT
        lgpd.record_data_processing(
            subject.id, "Arquivamento", "legal_obligation", ["pessoal"], "System"
        )
        lgpd.processing_records[0].processing_date = datetime.now() - timedelta(
            days=10000
        )
        expired = lgpd.check_data_retention()
        assert expired == []


class TestGenerateDataInventory:
    def test_inventory_returns_expected_structure(self, lgpd):
        inventory = lgpd.generate_data_inventory()
        assert "total_data_subjects" in inventory
        assert "total_processing_records" in inventory
        assert "classification_distribution" in inventory
        assert "retention_distribution" in inventory
        assert "legal_basis_distribution" in inventory
        assert "processing_purposes" in inventory
        assert "data_categories" in inventory

    def test_inventory_counts_subjects_and_records(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        lgpd.create_data_subject(data)
        subject = list(lgpd.data_subjects.values())[0]
        lgpd.record_data_processing(
            subject.id, "Marketing", "consent", ["pessoal"], "System"
        )
        inventory = lgpd.generate_data_inventory()
        assert inventory["total_data_subjects"] == 1
        assert inventory["total_processing_records"] == 1

    def test_inventory_classification_distribution(self, lgpd):
        data = {"name": "Joao", "email": "joao@example.com"}
        lgpd.create_data_subject(data)
        inventory = lgpd.generate_data_inventory()
        assert "internal" in inventory["classification_distribution"]
