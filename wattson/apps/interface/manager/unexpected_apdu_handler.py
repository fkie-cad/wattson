from logging import Logger

from wattson.util import ContextLogger, UnsupportedError, IEC104Exceptions

from wattson.apps.interface.util.messages import *
from wattson.iec104.common import APDU_verifier


class UnexpectedApduHandler:
    def __init__(self, parent_logger: Logger, raise_on_unexpected: bool = True,
                 raise_on_unsupported: bool = True):
        self.logger = parent_logger.getChild('UnexpectedApduH')
        self.raise_on_unexpected = raise_on_unexpected
        self.raise_on_unsupported = raise_on_unsupported

    def verify_send_apdu(self, apdu: APDU) -> bool:
        if isinstance(apdu, I_FORMAT):
            return self.verify_send_I_FORMAT(apdu)
        return True

    def verify_send_I_FORMAT(self, apdu: I_FORMAT) -> bool:
        e_type, cause = APDU_verifier.verify_send_I_FORMAT(apdu)

        if e_type is not None:
            self._log_and_raise_error_if_necessary((e_type, cause), True, apdu)
            return False
        return True

    def verify_recvd_apdu(self, apdu: APDU) -> bool:
        if isinstance(apdu, I_FORMAT):
            return self.verify_rcvd_I_FORMAT(apdu)
        return True

    def verify_rcvd_I_FORMAT(self, apdu: I_FORMAT) -> bool:
        e_type, cause = APDU_verifier.verify_rcvd_I_FORMAT(apdu)

        if e_type is not None:
            self._log_and_raise_error_if_necessary((e_type, cause), False, apdu)
            return False
        return True

    def _log_and_raise_error_if_necessary(self, err: Tuple[IEC104Exceptions, str], from_master: bool,
                                          apdu: APDU) -> None:
        e = f"[{err[1]}] "
        e += "Send" if from_master else "Rcvd"
        e += " unsupported" if isinstance(err[0], UnsupportedError) else " unexpected"
        e += f" apdu: {apdu}"

        self.logger.warning(e)
        if isinstance(err[1], UnsupportedError) and self.raise_on_unsupported:
            raise err[0](e)
        elif self.raise_on_unexpected:
            raise err[0](e)
