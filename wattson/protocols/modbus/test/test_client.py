import logging
import time
from typing import List, Optional

from pymodbus import ModbusException

from wattson.protocols.modbus.modbus_client import ModbusClient
from wattson.protocols.modbus.model.modbus_client_value_definition import ModbusClientValueDefinition
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType
from wattson.protocols.modbus.test.data_points import MODBUS_TEST_DATA_POINTS
from wattson.util import get_logger


def main():
    data_points = MODBUS_TEST_DATA_POINTS.copy()
    server_ip = "127.0.0.1"

    read_custom = True
    read_dps = True

    server_data = {
        "123": {
            "ip": server_ip,
            "port": 502,
        }
    }

    logger = get_logger("TestClient")
    logger.setLevel(logging.INFO)

    def on_value_update(value_definition: ModbusClientValueDefinition, old_value: Optional[ModbusValueType], new_value: Optional[ModbusValueType]):
        logger.info(f"{value_definition.data_point_identifier}: {new_value}     (was {old_value})")

    client = ModbusClient(server_data=server_data, logger=logger)
    client.set_data_points(data_points)
    client.set_on_value_update_callback(on_value_update)
    client.start(enable_polling=False)
    if not client.wait_until_connected(timeout=10):
        logger.error(f"Could not connect within 10 seconds")
        client.stop()
        return

    if read_dps:
        for data_point in data_points:
            identifier = data_point["identifier"]
            try:
                value = client.read_data_point(identifier)
                logger.info(f"Read {identifier}: {value}")
            except ModbusException as e:
                logger.error(f"{identifier}: {e}")

    manual_reads = [
        (ModbusTable.COIL, 5, 3)
    ]
    if read_custom:
        for table, address, count in manual_reads:
            logger.info(f"Reading from {table.name}: {address} // {count=}")
            data = client.read_from_table(list(server_data.keys())[0], modbus_table=table, address=address, count=count, unit_id=0)
            logger.info(f" Data: {data}")

    def on_write_done_callback(_value_def, _success, _exception_code, _values, _value):
        if not _success:
            logger.warning(f"FAILED writing {_value_def.data_point_identifier} - {_exception_code=}")
        else:
            logger.info(f"SUCCESS writing {_value_def.data_point_identifier} - {str(_value).ljust(10)} {_values}")

    def on_read_callback(_value_def, _success, _exception_code, _values, _value):
        if not _success:
            logger.warning(f"FAILED reading {_value_def.data_point_identifier}: {_exception_code} - {str(_value).ljust(10)} {_values}")
        else:
            logger.info(f"SUCCESS reading {_value_def.data_point_identifier} - {str(_value).ljust(10)} {_values}")

    time.sleep(1)
    for i in range(5):
        val = 10000 + i * 3
        logger.info(f"WRITING {val} to hr")
        client.write_data_point_callback("hr", val, on_write_done_callback)
        logger.info(f"Reading: {client.read_data_point('hr')}")
        logger.info(f"----")
        time.sleep(1)

    try:
        time.sleep(10)
    except KeyboardInterrupt:
        logger.error("Caught KeyboardInterrupt - stopping")
        client.stop()


if __name__ == "__main__":
    main()
