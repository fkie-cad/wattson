# RTU Logics

A description of different RTU Logics.

## Fake Switch Status

---

Send a fake switch status to the VCC.

### Example config

```yaml
switch.4.MEASUREMENT.is_closed:
  functions:
    - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.force_switch_status
      class: ForceSwitchStatus
      options:
        status: True
        interval: (0, 30)
    - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.force_switch_status
      class: ForceSwitchStatus
      options:
        status: False
        interval: (30, 60)

switch.4.CONFIGURATION.closed:
  functions:
    - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.force_switch_status
      class: ForceSwitchStatus
      options:
        status: True
        interval: (0, 30)
    - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.force_switch_status
      class: ForceSwitchStatus
      options:
        status: False
        interval: (30, 60)
```

This forces switch 4 to be closed during the first 30 seconds and to be open the next 30 seconds.

## Measurement Drift Off

---

Altering values based on Functions.

The values drift off as specified in the yaml configuration.

Functions can be combined by defining the same interval for them.

### Sine Drift Off

```yaml
849:
  bus.8.MEASUREMENT.voltage:
    functions:
      - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.sine_function
        class: SineFunction
        options:
          interval: (0, inf)
          frequency: .1
          scale: .1
        shift: 0
```

### Linear Sine Drift Off

```yaml
849:
  voltage:
    functions:
      - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.sine_function
        class: sineFunction
        options:
          interval: (0, inf)
          frequency: .1
          scale: .5
          shift: 0
      - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.linear_function
        class: LinearFunction
        options:
          interval: (0, inf)
          speed: .5
```

### Quadratic Slow Start Drift Off

```yaml
voltage:
  functions:
    - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.quadratic_function
      class: QuadraticFunction
      options:
        interval: (0, 120)
        speed: .01
        direction: "up"
        
    - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.quadratic_to_linear_function
      class: QuadraticToLinearFunction
      options:
        interval: (120, 240)
        speed: .01
        direction: "up"
```

## Circuit Breaker Safety

---

The logic checks which min / max values busses have
and which max values lines have if the connected switch has the safety enabled.
Based on the mode all switches connected to the bus are opened or just 
the one with the highest / lowest current on its line in case of an over / under voltage.

## Physical Attack Logic

---

Simulates an RTU being physically attacked by removing the corresponding
node, links, interfaces, etc.

## Spontaneous Logic

---

RTU Logic that issues spontaneous transmissions for changes of spontaneous data points