from typing import Dict, Any

from wattson.hosts.ccx.app_gateway.data_objects.ccx_report import CCXReport
from wattson.iec61850.iec61850_model import IEC61850Model


class CCXMmsReport(CCXReport):
    def __init__(
            self,
            report_name: str,
            report_reference: str,
            data_points: Dict[str, Any],
            data_attributes: Dict[str, Any],
            model: IEC61850Model
    ):
        super().__init__()
        self.report_name = report_name
        self.report_reference = report_reference
        self.data_points = data_points
        self.data_attributes = data_attributes
        self.model = model

    def to_dict(self) -> dict:
        return {
            'report_name': self.report_name,
            'report_reference': self.report_reference,
            'data_points': self.data_points,
            'data_attributes': self.data_attributes,
        }
