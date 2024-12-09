import time
from typing import Optional, Dict, TYPE_CHECKING, Type

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_query_type import WattsonQueryType
from wattson.cosimulation.models.facilities.facility import Facility
from wattson.cosimulation.models.model import Model
from wattson.cosimulation.models.model_manager import ModelManager
from wattson.cosimulation.models.remote_model import RemoteModel

from wattson.cosimulation.models.facilities.remote_facility import RemoteFacility


class RemoteModelManager(ModelManager):
    def __init__(self, wattson_client: WattsonClient):
        super().__init__()
        self._wattson_client = wattson_client
        self._update_timings: Dict[str, float] = {}
        self._last_general_sync_time = 0
        self._cache_timeout = 10

    @staticmethod
    def _get_remote_model_class(model_class: Type[Model]) -> Optional[Type[RemoteModel]]:
        if model_class == Facility:
            return RemoteFacility
        return None

    def _synchronize(self, model_type: Optional[str] = None, force_update: bool = False):
        if model_type is None:
            if not force_update:
                last_update = self._last_general_sync_time
                if last_update > time.time() - self._cache_timeout:
                    # Already synced
                    return
            query = WattsonQuery(query_type=WattsonQueryType.GET_MODELS, query_data={})
        else:
            # Get models of a specific type
            if not force_update:
                last_update = self._update_timings.get(model_type, 0)
                if last_update > time.time() - self._cache_timeout:
                    # Already synced
                    return
            # Sync specific model type
            query = WattsonQuery(query_type=WattsonQueryType.GET_MODELS, query_data={"model_type": model_type})
        response = self._wattson_client.query(query)
        if not response.is_successful():
            self._wattson_client.logger.warning(f"Could not synchronize remote models: {repr(response.data)}")
            return False
        for model_type, models_of_type in response.data.get("models", {}).items():
            for model_id, remote_representation in models_of_type.items():
                remote_model = remote_representation.to_wattson_remote_object(self._wattson_client)
                self.register_model(remote_model)

    def get_models(self, model_type: str, allow_empty: bool = True) -> Optional[Dict[str, RemoteModel]]:
        self._synchronize(model_type)
        return super().get_models(model_type, allow_empty)

    def get_facilities(self) -> Optional[Dict[str, 'RemoteFacility']]:
        return self.get_models("facility")
