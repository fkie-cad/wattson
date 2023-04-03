from enum import Enum


class ConfirmationStatus(Enum):
    WAITING_FOR_SEND = "Waiting for Master to send"
    SUCCESSFUL_SEND = "Successful Send"
    SUCCESSFUL_TERM = "Successful Termination"
    POSITIVE_CONFIRMATION = "Positive Confirmation"
    FAIL = "Failed"
    QUEUED = "Queued"
    CLIENT_QUEUED = "Queued on Client-Side"
    FINAL_RESP_RCVD = "Final Response"

