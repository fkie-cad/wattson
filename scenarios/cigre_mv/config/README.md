# Coordinator Logic Scripts

---

## Description

The scripts need to be referenced in `extensions/config.yml`.
The passing of arguments in `coord_logic_scripts` is optional.

```yaml
coord_config:
...
  coord_logic_scripts:
    - file: "config/FILENAME.py"
      class: "CLASSNAME"
      args: {"your arguments": "here"}
```

## Logics

---

### CoordCosPhi

---

TODO

### CircuitBreakerSafetyLogic

---

This logic implements protective technology.
For every powerflow all busses and lines are checked if their values are within the allowed range.
Busses are checked first, lines second.
Lines are only checked if no bus is outside the allowed range.
If any protective technology triggers a successful change (disabling of generators / loads, opening switches, ...) a new powerflow is requested immediately.

#### Handling of busses

---

The allowed range for busses is defined by 4 values:
1. "u>" indicating a high voltage (optional)
2. "u>>" indicating an overvoltage 
3. "u<" indicating a low voltage (optional)
4. "u<<" indicating an undervoltage

The logic tries to load the thresholds first from the `powernetwork.yml`.
It will then overwrite all existing thresholds with those from `extensions/config.yml`.
If not all 4 thresholds are given the respective bus will be added to the whitelist and has no protective technology.

In case of an overvoltage at a bus the logic will try to resolve this by:
1. Try to disable the generator at this bus.
2. If no generator can be disabled try to open a switch at the line that is connected to the bus that is the farthest away from any external grid.
3. If two busses have the same distance try to open a switch at the line with the highest power incoming.
4. If that doesn't work try next bus.

In case of an undervoltage at a bus the logic will try to resolve this by:
1. Try to disable the load with the highest electric charge which is still active.
2. If no load can be disabled try to open a switch at the line that is connected to the bus that is the farthest away from any external grid.
3. If two busses have the same distance try to open a switch at the line with the highest power outgoing.
4. If that doesn't work try next bus.

#### Handling lines

---

The allowed range for lines is defined by 2 values:
1. "i>" indicating a high current
2. "i>>" indicating an overcurrent

The logic tries to load the thresholds first from the `powernetwork.yml`.
It will then overwrite all existing thresholds with those from `extensions/config.yml`.
Thresholds from config can either be absolute values in kA or allowed deviation from maximum possible current.
If not all 2 thresholds are given the respective line will be added to the whitelist and has no protective technology.

In case of an overcurrent at a line the logic will try to resolve this by:
1. Opening a switch at the bus where the current comes from
2. If source bus has no switch try to open a switch at the bus where the current goes to
3. If that doesn't work try next line.

#### Example

---

The example below shows how to 
1. whitelist the busses with index 11 and 15 as well as the lines with index 2 and 4  
2. Overwrite values of bus 3 and line 1.

```yaml
coord_config:
...
  coord_logic_scripts:
    - file: "config/circuit_breaker_safety_logic.py"
      class: "CircuitBreakerSafetyLogic"
      args: {
        "whitelist": {
          "busses": [11, 15], 
          "lines": [2, 4]
        }, 
        "thresholds": {
          "busses": {3: "u>>": 1.0}, 
          "lines": {1: {"i>>": 0.75, "unit": "%"}}
        }
      }
```