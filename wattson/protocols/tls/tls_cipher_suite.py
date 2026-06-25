import enum
from typing import List, Optional

from powerowl.layers.network.configuration.protocols.tls_version import TlsVersion


class TlsCipherSuite(int, enum.Enum):
    """
    TLS Cipher Suite in accordance with mbedtls
    """
    TLS_RSA_WITH_NULL_MD5 = 0x01  # < Weak!
    TLS_RSA_WITH_NULL_SHA = 0x02  # < Weak!

    TLS_PSK_WITH_NULL_SHA = 0x2C  # < Weak!
    TLS_DHE_PSK_WITH_NULL_SHA = 0x2D  # < Weak!
    TLS_RSA_PSK_WITH_NULL_SHA = 0x2E  # < Weak!
    TLS_RSA_WITH_AES_128_CBC_SHA = 0x2F

    TLS_DHE_RSA_WITH_AES_128_CBC_SHA = 0x33
    TLS_RSA_WITH_AES_256_CBC_SHA = 0x35
    TLS_DHE_RSA_WITH_AES_256_CBC_SHA = 0x39

    TLS_RSA_WITH_NULL_SHA256 = 0x3B  # < Weak!
    TLS_RSA_WITH_AES_128_CBC_SHA256 = 0x3C  # < TLS 1.2
    TLS_RSA_WITH_AES_256_CBC_SHA256 = 0x3D  # < TLS 1.2

    TLS_RSA_WITH_CAMELLIA_128_CBC_SHA = 0x41
    TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA = 0x45

    TLS_DHE_RSA_WITH_AES_128_CBC_SHA256 = 0x67  # < TLS 1.2
    TLS_DHE_RSA_WITH_AES_256_CBC_SHA256 = 0x6B  # < TLS 1.2

    TLS_RSA_WITH_CAMELLIA_256_CBC_SHA = 0x84
    TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA = 0x88

    TLS_PSK_WITH_AES_128_CBC_SHA = 0x8C
    TLS_PSK_WITH_AES_256_CBC_SHA = 0x8D

    TLS_DHE_PSK_WITH_AES_128_CBC_SHA = 0x90
    TLS_DHE_PSK_WITH_AES_256_CBC_SHA = 0x91

    TLS_RSA_PSK_WITH_AES_128_CBC_SHA = 0x94
    TLS_RSA_PSK_WITH_AES_256_CBC_SHA = 0x95

    TLS_RSA_WITH_AES_128_GCM_SHA256 = 0x9C  # < TLS 1.2
    TLS_RSA_WITH_AES_256_GCM_SHA384 = 0x9D  # < TLS 1.2
    TLS_DHE_RSA_WITH_AES_128_GCM_SHA256 = 0x9E  # < TLS 1.2
    TLS_DHE_RSA_WITH_AES_256_GCM_SHA384 = 0x9F  # < TLS 1.2

    TLS_PSK_WITH_AES_128_GCM_SHA256 = 0xA8  # < TLS 1.2
    TLS_PSK_WITH_AES_256_GCM_SHA384 = 0xA9  # < TLS 1.2
    TLS_DHE_PSK_WITH_AES_128_GCM_SHA256 = 0xAA  # < TLS 1.2
    TLS_DHE_PSK_WITH_AES_256_GCM_SHA384 = 0xAB  # < TLS 1.2
    TLS_RSA_PSK_WITH_AES_128_GCM_SHA256 = 0xAC  # < TLS 1.2
    TLS_RSA_PSK_WITH_AES_256_GCM_SHA384 = 0xAD  # < TLS 1.2

    TLS_PSK_WITH_AES_128_CBC_SHA256 = 0xAE
    TLS_PSK_WITH_AES_256_CBC_SHA384 = 0xAF
    TLS_PSK_WITH_NULL_SHA256 = 0xB0  # < Weak!
    TLS_PSK_WITH_NULL_SHA384 = 0xB1  # < Weak!

    TLS_DHE_PSK_WITH_AES_128_CBC_SHA256 = 0xB2
    TLS_DHE_PSK_WITH_AES_256_CBC_SHA384 = 0xB3
    TLS_DHE_PSK_WITH_NULL_SHA256 = 0xB4  # < Weak!
    TLS_DHE_PSK_WITH_NULL_SHA384 = 0xB5  # < Weak!

    TLS_RSA_PSK_WITH_AES_128_CBC_SHA256 = 0xB6
    TLS_RSA_PSK_WITH_AES_256_CBC_SHA384 = 0xB7
    TLS_RSA_PSK_WITH_NULL_SHA256 = 0xB8  # < Weak!
    TLS_RSA_PSK_WITH_NULL_SHA384 = 0xB9  # < Weak!

    TLS_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xBA  # < TLS 1.2
    TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xBE  # < TLS 1.2

    TLS_RSA_WITH_CAMELLIA_256_CBC_SHA256 = 0xC0  # < TLS 1.2
    TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256 = 0xC4  # < TLS 1.2

    TLS_ECDH_ECDSA_WITH_NULL_SHA = 0xC001  # < Weak!
    TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA = 0xC004
    TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA = 0xC005

    TLS_ECDHE_ECDSA_WITH_NULL_SHA = 0xC006  # < Weak!
    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA = 0xC009
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA = 0xC00A

    TLS_ECDH_RSA_WITH_NULL_SHA = 0xC00B  # < Weak!
    TLS_ECDH_RSA_WITH_AES_128_CBC_SHA = 0xC00E
    TLS_ECDH_RSA_WITH_AES_256_CBC_SHA = 0xC00F

    TLS_ECDHE_RSA_WITH_NULL_SHA = 0xC010  # < Weak!
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA = 0xC013
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA = 0xC014

    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256 = 0xC023  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384 = 0xC024  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256 = 0xC025  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384 = 0xC026  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256 = 0xC027  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384 = 0xC028  # < TLS 1.2
    TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256 = 0xC029  # < TLS 1.2
    TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384 = 0xC02A  # < TLS 1.2

    TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 = 0xC02B  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 = 0xC02C  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256 = 0xC02D  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384 = 0xC02E  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 = 0xC02F  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 = 0xC030  # < TLS 1.2
    TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256 = 0xC031  # < TLS 1.2
    TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384 = 0xC032  # < TLS 1.2

    TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA = 0xC035
    TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA = 0xC036
    TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256 = 0xC037
    TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384 = 0xC038
    TLS_ECDHE_PSK_WITH_NULL_SHA = 0xC039
    TLS_ECDHE_PSK_WITH_NULL_SHA256 = 0xC03A
    TLS_ECDHE_PSK_WITH_NULL_SHA384 = 0xC03B

    TLS_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC03C  # < TLS 1.2
    TLS_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC03D  # < TLS 1.2
    TLS_DHE_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC044  # < TLS 1.2
    TLS_DHE_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC045  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_ARIA_128_CBC_SHA256 = 0xC048  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_ARIA_256_CBC_SHA384 = 0xC049  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_ARIA_128_CBC_SHA256 = 0xC04A  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_ARIA_256_CBC_SHA384 = 0xC04B  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC04C  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC04D  # < TLS 1.2
    TLS_ECDH_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC04E  # < TLS 1.2
    TLS_ECDH_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC04F  # < TLS 1.2
    TLS_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC050  # < TLS 1.2
    TLS_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC051  # < TLS 1.2
    TLS_DHE_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC052  # < TLS 1.2
    TLS_DHE_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC053  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_ARIA_128_GCM_SHA256 = 0xC05C  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_ARIA_256_GCM_SHA384 = 0xC05D  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_ARIA_128_GCM_SHA256 = 0xC05E  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_ARIA_256_GCM_SHA384 = 0xC05F  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC060  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC061  # < TLS 1.2
    TLS_ECDH_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC062  # < TLS 1.2
    TLS_ECDH_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC063  # < TLS 1.2
    TLS_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC064  # < TLS 1.2
    TLS_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC065  # < TLS 1.2
    TLS_DHE_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC066  # < TLS 1.2
    TLS_DHE_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC067  # < TLS 1.2
    TLS_RSA_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC068  # < TLS 1.2
    TLS_RSA_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC069  # < TLS 1.2
    TLS_PSK_WITH_ARIA_128_GCM_SHA256 = 0xC06A  # < TLS 1.2
    TLS_PSK_WITH_ARIA_256_GCM_SHA384 = 0xC06B  # < TLS 1.2
    TLS_DHE_PSK_WITH_ARIA_128_GCM_SHA256 = 0xC06C  # < TLS 1.2
    TLS_DHE_PSK_WITH_ARIA_256_GCM_SHA384 = 0xC06D  # < TLS 1.2
    TLS_RSA_PSK_WITH_ARIA_128_GCM_SHA256 = 0xC06E  # < TLS 1.2
    TLS_RSA_PSK_WITH_ARIA_256_GCM_SHA384 = 0xC06F  # < TLS 1.2
    TLS_ECDHE_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC070  # < TLS 1.2
    TLS_ECDHE_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC071  # < TLS 1.2

    TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC072
    TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC073
    TLS_ECDH_ECDSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC074
    TLS_ECDH_ECDSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC075
    TLS_ECDHE_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC076
    TLS_ECDHE_RSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC077
    TLS_ECDH_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC078
    TLS_ECDH_RSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC079

    TLS_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC07A  # < TLS 1.2
    TLS_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC07B  # < TLS 1.2
    TLS_DHE_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC07C  # < TLS 1.2
    TLS_DHE_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC07D  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC086  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC087  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC088  # < TLS 1.2
    TLS_ECDH_ECDSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC089  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC08A  # < TLS 1.2
    TLS_ECDHE_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC08B  # < TLS 1.2
    TLS_ECDH_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC08C  # < TLS 1.2
    TLS_ECDH_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC08D  # < TLS 1.2

    TLS_PSK_WITH_CAMELLIA_128_GCM_SHA256 = 0xC08E  # < TLS 1.2
    TLS_PSK_WITH_CAMELLIA_256_GCM_SHA384 = 0xC08F  # < TLS 1.2
    TLS_DHE_PSK_WITH_CAMELLIA_128_GCM_SHA256 = 0xC090  # < TLS 1.2
    TLS_DHE_PSK_WITH_CAMELLIA_256_GCM_SHA384 = 0xC091  # < TLS 1.2
    TLS_RSA_PSK_WITH_CAMELLIA_128_GCM_SHA256 = 0xC092  # < TLS 1.2
    TLS_RSA_PSK_WITH_CAMELLIA_256_GCM_SHA384 = 0xC093  # < TLS 1.2

    TLS_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC094
    TLS_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC095
    TLS_DHE_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC096
    TLS_DHE_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC097
    TLS_RSA_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC098
    TLS_RSA_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC099
    TLS_ECDHE_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC09A
    TLS_ECDHE_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC09B

    TLS_RSA_WITH_AES_128_CCM = 0xC09C  # < TLS 1.2
    TLS_RSA_WITH_AES_256_CCM = 0xC09D  # < TLS 1.2
    TLS_DHE_RSA_WITH_AES_128_CCM = 0xC09E  # < TLS 1.2
    TLS_DHE_RSA_WITH_AES_256_CCM = 0xC09F  # < TLS 1.2
    TLS_RSA_WITH_AES_128_CCM_8 = 0xC0A0  # < TLS 1.2
    TLS_RSA_WITH_AES_256_CCM_8 = 0xC0A1  # < TLS 1.2
    TLS_DHE_RSA_WITH_AES_128_CCM_8 = 0xC0A2  # < TLS 1.2
    TLS_DHE_RSA_WITH_AES_256_CCM_8 = 0xC0A3  # < TLS 1.2
    TLS_PSK_WITH_AES_128_CCM = 0xC0A4  # < TLS 1.2
    TLS_PSK_WITH_AES_256_CCM = 0xC0A5  # < TLS 1.2
    TLS_DHE_PSK_WITH_AES_128_CCM = 0xC0A6  # < TLS 1.2
    TLS_DHE_PSK_WITH_AES_256_CCM = 0xC0A7  # < TLS 1.2
    TLS_PSK_WITH_AES_128_CCM_8 = 0xC0A8  # < TLS 1.2
    TLS_PSK_WITH_AES_256_CCM_8 = 0xC0A9  # < TLS 1.2
    TLS_DHE_PSK_WITH_AES_128_CCM_8 = 0xC0AA  # < TLS 1.2
    TLS_DHE_PSK_WITH_AES_256_CCM_8 = 0xC0AB  # < TLS 1.2
    # The last two are named with PSK_DHE in the RFC, which looks like a typo

    TLS_ECDHE_ECDSA_WITH_AES_128_CCM = 0xC0AC  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_AES_256_CCM = 0xC0AD  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_AES_128_CCM_8 = 0xC0AE  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_AES_256_CCM_8 = 0xC0AF  # < TLS 1.2

    TLS_ECJPAKE_WITH_AES_128_CCM_8 = 0xC0FF  # < experimental

    # RFC 7905
    TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 = 0xCCA8  # < TLS 1.2
    TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 = 0xCCA9  # < TLS 1.2
    TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAA  # < TLS 1.2
    TLS_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAB  # < TLS 1.2
    TLS_ECDHE_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAC  # < TLS 1.2
    TLS_DHE_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAD  # < TLS 1.2
    TLS_RSA_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAE  # < TLS 1.2

    # RFC 8446, Appendix B.4
    TLS1_3_AES_128_GCM_SHA256 = 0x1301  # < TLS 1.3
    TLS1_3_AES_256_GCM_SHA384 = 0x1302  # < TLS 1.3
    TLS1_3_CHACHA20_POLY1305_SHA256 = 0x1303  # < TLS 1.3
    TLS1_3_AES_128_CCM_SHA256 = 0x1304  # < TLS 1.3
    TLS1_3_AES_128_CCM_8_SHA256 = 0x1305  # < TLS 1.3

    def is_tls_version(self, tls_version: TlsVersion) -> bool:
        if tls_version == TlsVersion.TLS_1_3:
            return self.name.startswith("TLS1_3")
        if tls_version == TlsVersion.TLS_1_2:
            if self.is_tls_version(TlsVersion.TLS_1_3):
                return False
            return any(x in self.name for x in ("SHA256", "SHA384", "GCM", "CCM", "CHACHA20"))
        return False

    def is_weak(self) -> bool:
        """
        A cipher suite is considered weak if:
        - it uses NULL encryption
        - it uses MD5 or SHA1
        """
        name = self.name
        hash = self.get_hash()
        return (
                "WITH_NULL_" in name
                or hash == "MD5"
                or hash == "SHA"
        )

    # ---------- suite listings ----------
    @classmethod
    def get_suites_for_tls_version(cls, tls_version: TlsVersion) -> List["TlsCipherSuite"]:
        return [s for s in cls if s.is_tls_version(tls_version)]

    @classmethod
    def get_tls_1_3_suites(cls) -> List["TlsCipherSuite"]:
        return TlsCipherSuite.get_suites_for_tls_version(TlsVersion.TLS_1_3)

    @classmethod
    def get_tls_1_2_suites(cls) -> List["TlsCipherSuite"]:
        return TlsCipherSuite.get_suites_for_tls_version(TlsVersion.TLS_1_2)

    @classmethod
    def get_null_ciphers(cls) -> List['TlsCipherSuite']:
        return [s for s in cls if not s.has_encryption()]

    # ---------- cryptographic properties ----------
    def has_encryption(self) -> bool:
        return "WITH_NULL_" not in self.name

    def get_encryption(self) -> Optional[str]:
        """
        Returns the bulk encryption algorithm or None if encryption is disabled.
        """
        if not self.has_encryption():
            return None

        name = self.name

        for alg in (
                "AES_128_GCM",
                "AES_256_GCM",
                "AES_128_CCM_8",
                "AES_256_CCM_8",
                "AES_128_CCM",
                "AES_256_CCM",
                "AES_128_CBC",
                "AES_256_CBC",
                "CAMELLIA_128_GCM",
                "CAMELLIA_256_GCM",
                "CAMELLIA_128_CBC",
                "CAMELLIA_256_CBC",
                "ARIA_128_GCM",
                "ARIA_256_GCM",
                "ARIA_128_CBC",
                "ARIA_256_CBC",
                "CHACHA20_POLY1305",
        ):
            if alg in name:
                return alg

        return None

    def get_hash(self) -> str:
        """
        Returns the hash function used by the cipher suite.
        TLS 1.3 always uses the hash defined in the suite name.
        """
        name = self.name

        for h in ("SHA384", "SHA256", "SHA", "MD5"):
            if name.endswith(h):
                return h

        # TLS 1.3 AEAD suites embed the hash
        if "SHA256" in name:
            return "SHA256"
        if "SHA384" in name:
            return "SHA384"

        # Fallback (should not happen for valid suites)
        return "UNKNOWN"

    def get_signature(self) -> Optional[str]:
        """
        Returns the signature algorithm if applicable.
        TLS 1.3 removes signature from cipher suite definition.
        """
        if self.is_tls_version(TlsVersion.TLS_1_3):
            return None

        name = self.name
        if "ECDSA" in name:
            return "ECDSA"
        if "RSA" in name:
            return "RSA"

        return None

    def get_key_exchange(self) -> Optional[str]:
        """
        Returns the key exchange algorithm.
        TLS 1.3 uses (EC)DHE implicitly and does not encode it in the name.
        """
        if self.is_tls_version(TlsVersion.TLS_1_3):
            return "DHE"

        name = self.name
        for kx in (
                "ECDHE",
                "ECDH",
                "DHE",
                "RSA_PSK",
                "DHE_PSK",
                "ECDHE_PSK",
                "PSK",
                "RSA",
                "ECJPAKE",
        ):
            if name.startswith(f"TLS_{kx}_") or f"_{kx}_" in name:
                return kx

        return None
