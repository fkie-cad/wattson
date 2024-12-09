# MTU Logics

A description of different MTU Logics.

## Block APDUs

---

Block certain APDUs and either:
1. WIP: Tell VCC the transmission was successful even though it wasn't. (send_callback?)
2. TODO: Tell VCC that a different value was transmitted and display the fake one. (update_datapoint_callback?)

## Block Datapoints

---

Block certain datapoints during certain intervals as defined in the yaml config.

## Force APDU transmission error

---

Force certain APDUs to have the positive flag equal False even though the transmission was successful.


## Fake switch status

---

An MTU version of the same RTULogic.

## Measurement Drift Off

---

Altering values based on Functions.

The values drift off as specified in the yaml configuration.

Functions can be combined by defining the same interval for them.

### Sine Drift Off

```yaml
bus.8.MEASUREMENT.voltage:
  functions:
    - module: wattson.hosts.rtu.logic.attack_logics.attack_functions.sine_function
      class: SineFunction
      options:
        interval: (0, inf)
        frequency: .1
        scale: .5
        shift: 0
```

### Linear Sine Drift Off

```yaml
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
        speed: .1
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


## Hide Physical Failure

Hides physical failure of an RTU (for example by the RTU Logic PhysicalAttackLogic)
by sending signals in a given interval to the VCC.