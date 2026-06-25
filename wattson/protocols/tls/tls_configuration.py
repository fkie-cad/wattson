import dataclasses
from typing import List

from powerowl.layers.network.configuration.protocols.tls_version import TlsVersion
from wattson.protocols.tls.tls_authentication_mode import TlsAuthenticationMode
from wattson.protocols.tls.tls_cipher_suite import TlsCipherSuite
from wattson.protocols.tls.tls_encryption_mode import TlsEncryptionMode
from wattson.protocols.tls.tls_validation_mode import TlsValidationMode


@dataclasses.dataclass(kw_only=True)
class TlsConfiguration:
    tls_version: TlsVersion = TlsVersion.NONE
    tls_authentication_mode: TlsAuthenticationMode = TlsAuthenticationMode.NONE
    tls_encryption_mode: TlsEncryptionMode = TlsEncryptionMode.NONE
    tls_validation_mode: TlsValidationMode = TlsValidationMode.NONE
    tls_expected_client_ids: List = dataclasses.field(default_factory=list)

    def configure_from_tls_version(self, tls_version: TlsVersion):
        self.tls_version = tls_version
        if self.tls_version == TlsVersion.NONE:
            self.tls_authentication_mode = TlsAuthenticationMode.NONE
            self.tls_encryption_mode = TlsEncryptionMode.NONE
            self.tls_validation_mode = TlsValidationMode.NONE
        elif self.tls_version == TlsVersion.TLS_1_2 or self.tls_version == TlsVersion.TLS_1_3:
            self.tls_authentication_mode = TlsAuthenticationMode.BIDIRECTIONAL_AUTHENTICATION
            self.tls_encryption_mode = TlsEncryptionMode.ENABLED
            # TODO: Use automated validation detection?
            self.tls_validation_mode = TlsValidationMode.CERTIFICATE_AUTHORITY

    def is_tls_enabled(self) -> bool:
        return self.tls_version != TlsVersion.NONE

    def is_secure_tls_enabled(self) -> bool:
        return self.tls_version.get_number() >= TlsVersion.TLS_1_2.get_number()

    def is_encryption_enabled(self) -> bool:
        return self.tls_encryption_mode == TlsEncryptionMode.ENABLED

    def derive_cipher_suites(self) -> List[TlsCipherSuite]:
        cipher_suites = TlsCipherSuite.get_suites_for_tls_version(self.tls_version)
        enabled_suites = []
        if self.is_encryption_enabled():
            for suite in cipher_suites:
                if suite.has_encryption() and not suite.is_weak():
                    enabled_suites.append(suite)
        else:
            for suite in cipher_suites:
                if not suite.has_encryption():
                    enabled_suites.append(suite)
        return enabled_suites

    def apply_overrides(self, overrides: dict, is_client: bool = False, is_server: bool = False):
        if "version" in overrides:
            self.tls_version = TlsVersion.parse_string(overrides["version"])
        if "encryption" in overrides and overrides["encryption"] is not None:
            self.tls_encryption_mode = TlsEncryptionMode.ENABLED if overrides["encryption"] else TlsEncryptionMode.NONE
        client_config = overrides.get("client", {})
        server_config = overrides.get("server", {})

        configs = []
        if is_client:
            configs.append(client_config)
        elif is_server:
            configs.append(server_config)

        auths = [(TlsAuthenticationMode.CLIENT_AUTHENTICATION, client_config), (TlsAuthenticationMode.SERVER_AUTHENTICATION, server_config)]
        for auth_mode, config in auths:
            if "authentication" in config and config["authentication"] is not None:
                if config["authentication"]:
                    self.tls_authentication_mode |= auth_mode
                else:
                    self.tls_authentication_mode &= ~auth_mode

        for config in configs:
            if "whitelist" in config and config["whitelist"] is not None:
                if config["whitelist"]:
                    self.tls_validation_mode |= TlsValidationMode.WHITELIST
                else:
                    self.tls_validation_mode &= ~TlsValidationMode.WHITELIST
            if "chain-validation" in config and config["chain-validation"] is not None:
                if config["chain-validation"]:
                    self.tls_validation_mode |= TlsValidationMode.CERTIFICATE_AUTHORITY
                else:
                    self.tls_validation_mode &= ~TlsValidationMode.CERTIFICATE_AUTHORITY
