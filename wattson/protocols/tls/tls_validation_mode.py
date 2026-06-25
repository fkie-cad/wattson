from enum import IntFlag


class TlsValidationMode(IntFlag):
    # Disable validation (if not authentication used)
    # Accept all certificates (if authentication used)
    NONE = 0
    # Use whitelist for accepted certificates
    WHITELIST = 1
    # Require certificates to be signed by CA
    CERTIFICATE_AUTHORITY = 2
    # Require both authentication types
    WHITELISTED_CA_SIGNED = 3

    def has(self, validation_mode: 'TlsValidationMode') -> bool:
        return bool(self & validation_mode)
