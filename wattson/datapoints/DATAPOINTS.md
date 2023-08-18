# Data Point Format Definition
*(2021-02-02)*

Data points represent information to be transmitted between different devices both, 
syntactically and semantically.
Each data point can transmit a sinlge, dedicated value.
In contrast to the original data point format, this information is neither 
bound to a single protocol nor to a single backend (or *information source*).


## Data Points of multiple Hosts (File Format)
The whole data point file has the following (YAML) format where `DATAPOINT` is a 
single **data point** according to the below definitions 
*or* a data point **identifier** (e.g., "163.22352"):

```yaml
datapoints:
  'hostid1':
    - DATAPOINT
    - DATAPOINT
    - ...
  'hostid2':
    - DATAPOINT
    - DATAPOINT
    - ...
  ...
```

In particular, the `datapoints.yml` contains an object with the single key 
`datapoints`, which maps to an object with host IDs as keys and a list of 
data points or data point identifiers as values.

## Basic Data Point Structure
```json
{
  "identifier": UNIQUE_ID,
  "description": Optional(str),
  "value": Optional(str, int, float, bool), // The initial value
  
  "protocol": PROTOCOL_NAME,
  "protocol_data": PROTOCOL_DATA,
    
  "providers": {
    "sources": Optional([SOURCE_PROVIDER]),
    "targets": Optional([TARGET_PROVIDER])
  },
  
  "coupling": Optional(SNIPPET)
}
```

### (Unique) ID
The ID is a **unique string**, i.e., each data point (even accross multiple hosts) 
has globally unique ID.
While the ID might serve as a semantic provider, its form is not further 
restricted by this format definition. 

---
### Protocols
The protocol-related fields provide information on the communication protocol 
that this datapoint is transmitted with. The information consists of the 
**protocol name**, i.e., an identifier for the protocol, and the 
**protocol data** which gives protocol-specific information (e.g., field contents).

#### PROTOCOL_NAME
A **string** that identifies the protocol that this data point is transmitted over.
For instance, `"60870-5-104"` indicates the usage of the IEC 60870-5-104 protocol.
The data point format definition does not further restrict the values for 
`PROTOCOL_NAME` besides its data type (string).

*Conventions*
* For IEC 60870-5-104, the protocol name `"60870-5-104"` is used
* For Modbus over TCP, the protocol name `"MODBUS/TCP"` is used
* For protocols from the IEC 61850 family, protocol names of the form `"61850-X-Y"` are used

#### PROTOCOL_DATA
An **object** that, depending on the assigned protocol, provides additional 
information for data points transmission.
Below, a non-exhaustive list of protocol data definitions is given (To be extended)

##### IEC 60870-5-104
<table>
<tr>
<th>Format</th>
<th>Example</th>
</tr>
<tr>
<td>

```json
{
  "coa": int,
  "ioa": int,
  "cot": int,
  "type_id": int,
  "direction": str
}
```
</td>
<td>

```json
{
  "coa": 163,
  "ioa": 22351,
  "cot": 3,
  "type_id": 13,
  "direction": "monitoring"
}
```
</td>
</tr>
</table>

##### MODBUS / TCP
TBD

##### IEC 61850
TBD

---
### Snippets
**Snippets** are short code snippets that are used to transform and combine values.

* The notation `SNIPPET[X]` denotes a snippet that gets an external variable `X` as input
* The notation `SNIPPET[V, X1..XN]` denotes a snippet that gets various variables as input, named `V`, `X1`, `X2`, ..., `XN` where `N` depends on the context.

**Snippets are executed via the `eval` function - so use them with caution!**

An empty snippet **always** returns the unmodified first input variable or, 
if no variable is given, 0.

---
### Providers
Data point providers are organized as **sources** and **targets**.
Each (optional) key maps to a **list** of **PROVIDER**s.

Providers define value sources and targets.
In particular, providers define which value (sources) should be transmitted as the 
data point's value and, optionally, which (local) "registers" (targers) should 
be modified.

**Source Providers** are Providers that a value is read from while 
**Target Providers** are Providers that are actively written to.

Each `PROVIDER` is an **object** consisting of a **PROVIDER_TYPE** and 
**PROVIDER_DATA** as follows.

```json
{
  "provider_type": str,
  "provider_data": object,
  "coupling": Optional(SNIPPET[V, X1..XN]), // TARGET PROVIDER ONLY
  "transform": Optional(SNIPPET[X])         // SOURCE PROVIDER ONLY
}
```

#### Source Providers
Source Providers are used as inputs for data point values 
as well as for Target Providers.

The have an (optional) `transform` snippet that allows to transform the 
provider's value (`X`) before using it for a data point, further transformations, or as
input for target providers.

*Example*  
A provider that would naturally return `X=27` can be transformed to return the
third root of its value by adding the `transform` snippet `X**(1/3)`.

Hence, for a data point's value coupling, the input value of said source provider 
would be `3`.
The same applies to the input to the `coupling` snippet of Target Providers. 

#### Target Providers
Target Providers are used to store (transmitted and potentially transformed) 
values.
As their input, they take a transmitted value `V` and `n` further values 
`X1` to `XN` originating from the `n` source providers defined for the same
data point.

Their definition allows to change multiple values at once and write different 
values to different targets.

*Example*
* The data point transmits a value `V` of 1.
* The data point has source provider that reads the value `X1=90` from a local register
* The data point has source provider that reads the value `X2=40` from a pandapower field `p`

The target provider is defined to write to the same pandapower field `p`.  
The target provider uses the `coupling` snippet `X1 if V==1 else X2`.  
Hence, the new value of the pandapower field `p` is changed to the local register's 
value if and only if the data point transmitted the value `1`. 
Otherwise, the original value of `p` is maintained.

*This example is also given in the example section as 
"Conditional Target Provider"*.

#### PROVIDER_TYPE
A **string** that identifies the type of the provider. Common providers are
* `"static"`
* `"register"`
* `"pandapower"`

The data point format definition does not further restrict the possible 
provider types.

#### PROVIDER_DATA
An **object** that provides all information for the respective provider type.
The format is not further restricted.
Providers might use **SNIPPET**s as defined below.

Below, example definitions for *three* provider types are given.

##### Static Provider
Use a constant (base) value and, optionally, apply a transformation (e.g., noise).

<table>
<tr>
<th>Format</th>
<th>Example</th>
</tr>
<tr>
<td>

```json
{
  "value": Union(int, str, float, bool),
  "transform": Optional(SNIPPET[X])
}
```
</td>
<td>

```json
{
  "value": 50,
  "transform": "X + random.uniform(-0.02*X, 0.02*X)"
}
```
</td>
</tr>
</table>

##### Local Register
Store value to or read from a (virtual) local register with a given name. 
In case the register has not yet been initialized, a default value is required.

<table>
<tr>
<th>Format</th>
<th>Example</th>
</tr>
<tr>
<td>

```json
{
  "name": str,
  "default": Union(int, str, float, bool),
}
```
</td>
<td>

```json
{
  "name": "R1",
  "default": 42
}
```
</td>
</tr>
</table>

##### Pandapower Backend
Read values from or write to a pandapower simulation.

<table>
<tr>
<th>Format</th>
<th>Example</th>
</tr>
<tr>
<td>

```json
{
  "pp_table": str,
  "pp_column": str,
  "pp_index": int
}
```
</td>
<td>

```json
{
  "pp_table": "res_line",
  "pp_column": "p_to_mw",
  "pp_index": 14
}
```
</td>
</tr>
</table>

### Behavior
#### Reading
For reading a data point, the default case is a single source provider 
with value `X`.  
Hence, the implicit snippet for the data point's `coupling` attribute 
is `X` and can be omitted.

*See the "Reading a single unmodified Pandapower Value" Example*

#### Writing
For writing a data point, the default case is that the transmitted value `V`
is (unmodified) written to a target provider.
The target providers implicit snippet `SNIPPET[V,X1,..XN]` would return `V` as
default value and hence can be omited.

*See the "Writing the transmitted Value to a single Provider" Example*

---

# Examples
## Reading a single unmodified Pandapower Value
Note that we can omit the provider's `transform` snippet as well as the 
data point's `coupling` snippet.

```json
{
  "identifier": "163.22350",
  "description": "Power transmission of line 14",
  
  "protocol": "60870-5-104",
  "protocol_data": {
    "coa": 163,
    "ioa": 22350,
    "cot": 1,
    "type_id": 13,
    "direction": "monitoring"
  },
  
  "providers": {
    "sources": [
      {
        "provider_type": "pandapower",
        "provider_data": {
          "pp_table": "res_line",
          "pp_column": "p_to_mw",
          "pp_index": 14
        }
      }
    ]
  }
}
```

## Writing the transmitted Value to a single Provider
Note that we can omit the target provider's `coupling` attribute as, per default,
it returns `V`, i.e., the originally transmitted value.

```json
{
  "identifier": "163.22351",
  "description": "Switch 17 closed",
  
  "protocol": "60870-5-104",
  "protocol_data": {
    "coa": 163,
    "ioa": 22351,
    "cot": 6,
    "type_id": 1,
    "direction": "control"
  },
  
  "providers": {
    "targets": [
      {
        "provider_type": "pandapower",
        "provider_data": {
          "pp_table": "switch",
          "pp_column": "closed",
          "pp_index": 17
        }
      }
    ]
  }
}
```


## Conditional Target Provider
The controlling station configures a register `R1` to contain a target value 
for the power output percentage of a generator (`sgen` with ID `1`).  
Instead of directly writing to the pandapower field, we model a specific behavior, 
where the change is only applied once the value `1` is written to another 
data point which we define below to implement this behavior.

The `coupling` of the target provider gets the input `V` (transmitted value), 
`X1` (Value of register R1), and `X2` (Value of pandapower `sgen.p_mw.1`) and
writes `X1` to `sgen.p_mw.1` iff `V == 1`.

```json
{
  "identifier": "163.22352",
  "description": "Activate SGEN 1 power percentage",
  
  "protocol": "60870-5-104",
  "protocol_data": {
    "coa": 163,
    "ioa": 22352,
    "cot": 6,
    "type_id": 1,
    "direction": "control"
  },
  
  "providers": {
    "sources": [
      {
        "provider_type": "register",
        "provider_data": {
          "name": "R1",
          "default": 0
        }
      },
      {
        "provider_type": "pandapower",
        "provider_data": {
          "pp_table": "sgen",
          "pp_column": "p_mw",
          "pp_index": 1
        }
      }
    ],
    "targets": [
      {
        "provider_type": "pandapower",
        "provider_data": {
          "pp_table": "sgen",
          "pp_column": "p_mw",
          "pp_index": 1
        },
        "coupling": "X1 if V==1 else X2"
      }
    ]
  }
}
```