import threading
import traceback
from typing import Optional, Callable, Dict, Any

import pyprctl
from powerowl.layers.powergrid import PowerGridModel
from powerowl.layers.powergrid.elements import GridElement
from powerowl.layers.powergrid.values.grid_value import GridValue

from wattson.powergrid.profiles.profile_provider import ProfileLoaderFactory
from wattson.util import get_logger


class SimulationThread(threading.Thread):
    """This thread handles simulating the power grid, i.e., calculating the current grid state."""
    def __init__(
            self,
            power_grid_model: PowerGridModel, *,
            iteration_required_event: Optional[threading.Event] = None,
            automatic_simulation_interval_seconds: float = 10,
            on_iteration_start_callback: Optional[Callable[[], None]] = None,
            on_iteration_sync_callback: Optional[Callable[[bool], None]] = None,
            on_iteration_completed_callback: Optional[Callable[[bool], None]] = None,
            on_value_update_callback: Optional[Callable[[GridValue, Any, Any], None]] = None,
            on_value_change_callback: Optional[Callable[[GridValue, Any, Any], None]] = None,
            on_value_state_change_callback: Optional[Callable[[GridValue], None]] = None,
            on_protection_equipment_triggered_callback: Optional[Callable[[GridElement, str], None]] = None,
            on_protection_equipment_cleared_callback: Optional[Callable[[GridElement, str], None]] = None,

            **kwargs
            ):
        super().__init__()
        self.logger = get_logger("SimulationThread", "SimulationThread")
        self.power_grid_model = power_grid_model
        self.power_grid_model.set_on_simulation_iteration_synchronization(on_iteration_sync_callback)
        self.power_grid_model.set_on_protection_equipment_triggered(on_protection_equipment_triggered_callback)
        self.power_grid_model.set_on_protection_equipment_cleared(on_protection_equipment_cleared_callback)
        self.power_grid_model.set_option("enable_storage_processing", kwargs.get("enable_storage_processing", True))
        self.power_grid_model.set_option("enable_protection_emulation", kwargs.get("enable_protection_emulation", False))
        self.power_grid_model.set_option("protection_trigger_delay_seconds", kwargs.get("protection_trigger_delay_seconds"))
        self.power_grid_model.set_option("protection_trigger_threshold_factor", kwargs.get("protection_trigger_threshold_factor"))

        self._terminate_requested = threading.Event()
        if iteration_required_event is not None:
            self._iteration_required = iteration_required_event
        else:
            self._iteration_required = threading.Event()
        self._interval = automatic_simulation_interval_seconds
        self._on_iteration_start_callback = on_iteration_start_callback
        self._on_iteration_sync_callback = on_iteration_sync_callback
        self._on_iteration_complete_callback = on_iteration_completed_callback
        self._on_value_update_callback = on_value_update_callback
        self._on_value_change_callback = on_value_change_callback
        self._on_value_state_change_callback = on_value_state_change_callback
        self._add_on_value_change_callbacks()
        self._last_run = 0
        self.ready_event = threading.Event()
        self._initial_configuration_applied_event: Optional[threading.Event] = None
        self.power_grid_model.prepare_simulator()

    def set_iteration_required(self):
        self._iteration_required.set()

    def _add_on_value_change_callbacks(self):
        for e_type in self.power_grid_model.elements.keys():
            for element in self.power_grid_model.get_elements_by_type(e_type):
                grid_value: GridValue
                for value_name, grid_value in element.get_grid_values():
                    grid_value.add_on_set_callback(self._on_value_update)
                    grid_value.add_on_state_change_callback(self._on_value_state_change)

    def _on_value_update(self, grid_value: GridValue, old_value: Any, new_value: Any):
        if self._on_value_update_callback is not None:
            self._on_value_update_callback(grid_value, old_value, new_value)

        if self._on_value_change_callback is not None:
            if old_value != new_value:
                self._on_value_change_callback(grid_value, old_value, new_value)

    def _on_value_state_change(self, grid_value: GridValue):
        if self._on_value_state_change_callback is None:
            return
        self._on_value_state_change_callback(grid_value)
        
    def start(self, wait_for_event: Optional[threading.Event] = None):
        self._initial_configuration_applied_event = wait_for_event
        super().start()

    def stop(self, timeout: Optional[float] = None):
        """
        Requests stopping the simulation thread and joins the thread for the given timeout in seconds.
        After this method, is_alive() can be used to determine whether the thread actually stopped.

        Args:
            timeout (Optional[float], optional):
                The (optional) timeout for joining the thread.
                (Default value = None)
        """
        self._terminate_requested.set()
        self._iteration_required.set()
        self.join(timeout=timeout)

    def run(self) -> None:
        """
        The actual simulation calculation is done in a thread.
        :return:

        """
        pyprctl.set_name("W/PG/Sim")
        ready_event_set = False

        if self._initial_configuration_applied_event is not None:
            # Wait for initial values to be set (e.g., by the profile provider)
            while not self._terminate_requested.is_set():
                if self._initial_configuration_applied_event.wait(2):
                    break
                self.logger.info(f"Still waiting for initial configuration event...")

        self.logger.info(f"Simulation thread initialized")

        while not self._terminate_requested.is_set():
            try:
                self._on_iteration_start()
                self.logger.debug("Starting power grid simulation iteration")
                self.power_grid_model.simulate()
                self.logger.debug("Done with power grid simulation iteration")
                self._on_iteration_complete(True)
                if not self.ready_event.is_set():
                    self.ready_event.set()
            except Exception as e:
                self.logger.error(e)
                self.logger.error(traceback.format_exc())
                self._on_iteration_complete(False)
            self._iteration_required.wait(self._interval)
            self._iteration_required.clear()

    def _on_iteration_complete(self, success: bool):
        if self._on_iteration_complete_callback is not None:
            self._on_iteration_complete_callback(success)

    def _on_iteration_sync(self, success: bool):
        if self._on_iteration_sync_callback is not None:
            self._on_iteration_sync_callback(success)

    def _on_iteration_start(self):
        if self._on_iteration_start_callback is not None:
            self._on_iteration_start_callback()
