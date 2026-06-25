import logging
import time
from typing import List

from pymodbus.constants import ExcCodes
from pymodbus.pdu import ModbusPDU

from powerowl.layers.network.configuration.protocols.protocol_name import ProtocolName
from wattson.protocols.modbus.modbus_server import ModbusServer
from wattson.protocols.modbus.model.modbus_server_value_definition import ModbusServerValueDefinition
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType
from wattson.protocols.modbus.test.data_points import MODBUS_TEST_DATA_POINTS


def main():
    data_points = MODBUS_TEST_DATA_POINTS.copy()

    bind_ip = "127.0.0.1"
    bind_ip = "0.0.0.0"
    server = ModbusServer(bind_ip=bind_ip, bind_port=502)
    server.logger.setLevel(logging.DEBUG)

    server.set_data_points(data_points)

    server.set_on_client_connect(lambda ip, port: True)

    def on_receive_pdu(ip: str, port: int, pdu: ModbusPDU):
        print(f"Received PDU from {ip}:{port}")
        print(f"   Address:  {pdu.address}")
        print(f"   Count:    {pdu.count}")
        print(f"   Function: {pdu.function_code}")
        print("")

    def on_sent_pdu(ip: str, port: int, pdu: ModbusPDU):
        print(f"Sent PDU to {ip}:{port}")
        print(f"   Address:  {pdu.address}")
        print(f"   Count:    {pdu.count}")
        print(f"   Regs:     {pdu.registers}")
        print(f"   Bits:     {pdu.bits}")

    def on_before_read(_value_definition: ModbusValueDefinition) -> bool | ExcCodes:
        print(f"Read request for {_value_definition.data_point_identifier}")
        if _value_definition.is_bool():
            _value_definition.set_value(not _value_definition.get_value())
        elif _value_definition.is_float() and not _value_definition.is_writable():
            _value_definition.set_value(time.time())
        return True

    write_id = 0

    def on_before_write(_value_definition: ModbusValueDefinition, values: List[int] | List[bool]) -> bool | ExcCodes:
        nonlocal write_id
        if not _value_definition.is_writable():
            return ExcCodes.NEGATIVE_ACKNOWLEDGE
        print(f"Write request for {_value_definition.data_point_identifier}: {values}")
        if len(values) != _value_definition.register_width:
            print("Cannot write partial value")
            return ExcCodes.DEVICE_FAILURE
        write_id += 1
        if write_id % 2 == 0:
            print("Rejecting write")
            return ExcCodes.NEGATIVE_ACKNOWLEDGE
        return True

    def on_write(_value_definition: ModbusValueDefinition, values: List[int] | List[bool], value: ModbusValueType) -> bool | ExcCodes:
        print(f"Value Written: {_value_definition.data_point_identifier}: {value}")
        return True

    server.set_on_receive_pdu(on_receive_pdu)
    server.set_on_sent_pdu(on_sent_pdu)
    server.set_on_before_read(on_before_read)
    server.set_on_before_write(on_before_write)
    server.set_on_value_write(on_write)
    server.start()

    if False:
        for value_definition in server._value_definitions.values():
            print(f"{value_definition.data_point_identifier} - {value_definition.start_address}")
            print(f"    {value_definition.get_value()}")
            print(f"    {value_definition.get_raw_value()}")
        print("Started server - now waiting")
        print(f"C: {server.get_value('c')}")
        server.set_value("c", True)
        print(f"C: {server.get_value('c')}")

        server.set_value("di", True)

        print(f"HR: {server.get_value('hr')}")
        print(f"HR Raw: {server.get_value_definition('hr').get_raw_value()}")
        print(f"{server.set_value('hr', 42.136)}")
        print(f"HR: {server.get_value('hr')}")
        print(f"HR Raw: {server.get_value_definition('hr').get_raw_value()}")

        for unit_id in server.get_unit_list():
            print("")
            print(f"Unit {unit_id} Memory")
            print(f"  COILS")
            print(repr(server.get_unit_memory(unit_id).coils))
            print(f"  REGISTERS")
            print(repr(server.get_unit_memory(unit_id).registers))

    try:
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        server.stop()
        print("Exiting")


if __name__ == "__main__":
    main()
