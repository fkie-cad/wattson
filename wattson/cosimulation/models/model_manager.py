from pathlib import Path
from typing import List, Dict, Optional, Type, Union

import yaml

from wattson.cosimulation.control.interface.wattson_query_handler import WattsonQueryHandler
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_query_type import WattsonQueryType
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.models.model import Model


class ModelManager(WattsonQueryHandler):
    def __init__(self):
        self._models = {}

    def get_model_types(self) -> List[str]:
        return list(self._models.keys())

    def get_models(self, model_type: str, allow_empty: bool = True) -> Optional[Dict[str, Model]]:
        if model_type in self._models:
            return self._models.get(model_type)
        if allow_empty:
            return {}
        return None

    def get_model(self, model_type: str, model_id: str) -> Optional[Model]:
        return self.get_models(model_type).get(model_id)

    def register_model(self, model: Model):
        self._models.setdefault(model.get_model_type(), {})[model.get_id()] = model

    def load_from_file(self, model_class: Type[Model], file: Path) -> bool:
        if not file.exists():
            print("File not found")
            return False
        with file.open("r") as f:
            contents = yaml.load(f, Loader=yaml.Loader)
        if not isinstance(contents, dict):
            print("Invalid file content")
            print(repr(contents))
            return False
        for model_id, model_dict in contents.items():
            model = model_class.load_from_dict(model_dict)
            self.register_model(model)
        return True

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        return query.query_type in [WattsonQueryType.GET_MODELS]

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        if not self.handles_simulation_query_type(query):
            return None

        if query.query_type == WattsonQueryType.GET_MODELS:
            model_type = query.query_data.get("model_type")
            models = {}
            if model_type is None:
                model_types = self.get_model_types()
                for m_type in model_types:
                    self.get_models(m_type, allow_empty=True)
            else:
                models[model_type] = self.get_models(model_type, allow_empty=True)
            response_models = {}
            for m_type, models_of_type in models.items():
                response_models[m_type] = {}
                for model_id, model in models_of_type.items():
                    response_models[m_type][model_id] = model.to_remote_representation()
            response = WattsonResponse(True, {"models": response_models})
            return response
