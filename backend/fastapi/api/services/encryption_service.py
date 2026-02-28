import base64
import os
import contextvars
from typing import Optional
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy.types import TypeDecorator, Text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging

logger = logging.getLogger(__name__)

# Context variables to hold current user's DEK and ID globally for the current async task
current_dek = contextvars.ContextVar('current_dek', default=None)
current_user_id = contextvars.ContextVar('current_user_id', default=None)

# In production, this MUST come from a secure vault (KMS / HashiCorp Vault)
MASTER_KEY_STR = os.getenv("ENCRYPTION_MASTER_KEY", "b33945de21b7ebd25e171542fba861f22e70eade98aa80ce79015c7ee2f27bf2")
# Ensure 32 bytes
MASTER_KEY = MASTER_KEY_STR.encode('utf-8')[:32].ljust(32, b'\0')

class EncryptionService:
    @staticmethod
    def generate_dek() -> bytes:
        return AESGCM.generate_key(bit_length=256)

    @staticmethod
    def wrap_dek(dek: bytes) -> str:
        aesgcm = AESGCM(MASTER_KEY)
        nonce = os.urandom(12)
        wrapped = aesgcm.encrypt(nonce, dek, None)
        return base64.b64encode(nonce + wrapped).decode('utf-8')

    @staticmethod
    def unwrap_dek(wrapped_dek_str: str) -> bytes:
        aesgcm = AESGCM(MASTER_KEY)
        raw = base64.b64decode(wrapped_dek_str)
        nonce, wrapped = raw[:12], raw[12:]
        return aesgcm.decrypt(nonce, wrapped, None)

    @staticmethod
    def encrypt_data(plaintext: str, dek: bytes) -> str:
        if not plaintext:
            return plaintext
        aesgcm = AESGCM(dek)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, plaintext.encode('utf-8'), None)
        return "ENC:" + base64.b64encode(nonce + ciphertext).decode('utf-8')

    @staticmethod
    def decrypt_data(ciphertext_str: str, dek: bytes, log_audit: bool = True) -> str:
        if not ciphertext_str or not str(ciphertext_str).startswith("ENC:"):
            return ciphertext_str
        
        try:
            raw = base64.b64decode(ciphertext_str[4:])
            nonce, ciphertext = raw[:12], raw[12:]
            aesgcm = AESGCM(dek)
            plaintext = aesgcm.decrypt(nonce, ciphertext, None).decode('utf-8')
            
            # Application-Level Audit Logging (#1105)
            if log_audit:
                user_id = current_user_id.get()
                if user_id:
                    try:
                        from .kafka_producer import get_kafka_producer
                        from datetime import datetime, UTC
                        producer = get_kafka_producer()
                        producer.queue_event({
                            "type": "DATA_ACCESS",
                            "entity": "JournalEntry",
                            "entity_id": str(user_id),
                            "payload": {"action": "decrypted_sensitive_content"},
                            "user_id": user_id,
                            "timestamp": datetime.now(UTC).isoformat()
                        })
                    except Exception as e:
                        logger.error(f"Audit log push failed on decryption: {e}")
                        
            return plaintext
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return "<DECRYPTION_FAILED>"

    @staticmethod
    async def get_or_create_user_dek(user_id: int, db: AsyncSession) -> bytes:
        from ..models import UserEncryptionKey
        stmt = select(UserEncryptionKey).filter_by(user_id=user_id)
        result = await db.execute(stmt)
        record = result.scalar_one_or_none()
        
        if record:
            return EncryptionService.unwrap_dek(record.wrapped_dek)
        
        # Create new DEK
        dek = EncryptionService.generate_dek()
        wrapped = EncryptionService.wrap_dek(dek)
        new_record = UserEncryptionKey(user_id=user_id, wrapped_dek=wrapped)
        db.add(new_record)
        await db.commit()
        return dek

class EncryptedString(TypeDecorator):
    """
    Custom SQLAlchemy TypeDecorator (#1105).
    Transparently handles AEAD encryption on write and decryption on read.
    Requires `current_dek` ContextVar to be set by Auth Middleware.
    """
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
            
        dek = current_dek.get()
        if not dek:
            logger.warning("No User DEK found in ContextVar. Aborting encryption.")
            raise ValueError("Application-level encryption requires active User DEK context.")
            
        if isinstance(value, str) and value.startswith("ENC:"):
            return value
            
        return EncryptionService.encrypt_data(str(value), dek)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
            
        if not value.startswith("ENC:"):
            return value
            
        dek = current_dek.get()
        if not dek:
            # Mask data to prevent plaintext leakage in insecure contexts
            return "<ENCRYPTED_DATA: DEK Context Required>"
            
        return EncryptionService.decrypt_data(value, dek)
