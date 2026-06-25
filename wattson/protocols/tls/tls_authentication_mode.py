from enum import IntFlag


class TlsAuthenticationMode(IntFlag):
    NONE = 0
    CLIENT_AUTHENTICATION = 1
    SERVER_AUTHENTICATION = 2
    BIDIRECTIONAL_AUTHENTICATION = 3

    def should_server_authenticate_itself(self) -> bool:
        return bool(self & TlsAuthenticationMode.SERVER_AUTHENTICATION)

    def should_client_authenticate_itself(self) -> bool:
        return bool(self & TlsAuthenticationMode.CLIENT_AUTHENTICATION)
