# Application-Layer connection to an MTU

Currently, the MTU specifies both a Monitoring-only and a Command-only pipeline.
The MTU opens two ZMQ servers correspondingly.
The command pipeline is open to future changes to best support multiple applications connecting at the same time
    and their requirements.
The Monitoring pipeline forwards via a Pub-Sub model:
- repacked RTU-initiated IEC104 packets
- status updates for previously send commands, e.g., for ACTCON, ACTTERM
- sharing of MTU-initiated **and** subscriber initiated IEC104 traffic producing commands after sending

Before sending commands, a two-message handshake is necessary.

#### MTU connection 
Default server-side port:
- ProcessInfoMonitoring-only: 5580
- Write-only: 5581

Default server-side IP:
- 127.0.0.1


#### Subscription policy
[Subscription Policies](FCS/MTU_subscriptions/configs) gives two examples of *subscription policies*. 
These configure in part for the entire MTU which information are going to be published if set to `True`.
Entries are stated in **bold** if they are not or not entirely implemented.
They are ~~striked~~ if discouraged. 
Enabling striked ones might result in Errors being raised or the MTU's msg cache becoming corrupted.
- **S-Frames:** S-format APDUs
- **U-Frames:** U-format APDUs
- acks `default=True`: forward ACTCON/ ACTTERM `(cot in (7, 8))` replies (Future DEACTCON/ DEACTTERM)
- **combine_IOs:** If it can be known which IOs (from >= 1 ASDU) are necessary to fulfill a command, 
                    send all in one message
- ~~extract-val_from-raw:~~ extract values from the raw callback handlers for the c104 repository
- **independent clock-synch:** only forward clock-synch (C_CS) information if initiated by a subscriber's command
- ~~handle_cot_5_raw:~~ handle ASDUs with cot 5 (ACTIVATION) in c104's raw callback (would allow analysing
them if not solely send through the `point.transmit()` function of the corresponding Master)
- ~~handle_cot_20_dps_raw~~ handle datapoint entries send with COT in raw callback
- ~~handle_monitoring_initiated_raw~~ handle any IEC104 messages in c104 raw callback if not reply to a prior MTU packet
 (Periodic updates, etc.)
 - ignore_unknown_cot_dp_callbacks `default=True`: Ignore if ``cot==45`` when receiving an IO from a MTU
 - ignore_quality `default=False`: Ignore if the IOs quality is bad, e.g., because of an overflow error
    - WARNING: 7.2.6.3 (IEC101) describes the quality bits. If set to `INVALID`, the send IO `may` be wrong
        and `cannot be used`. A more proper differentiation for the subscriptions requires a further breakdown
        into the individual qualitz bits to be ignored.
- **remove_actterm_raw:** Remove cache entries with ACTTERM status for messages once this is received;
    thus, rendering backwards reference impossible.
    


### Messages
The MTU sends and should receive serialised JSON messages.
These messeges were mostly split in correspondence with the `IEC 60870-5-104` standard.
Those messages that do not directly belong to this standard carry only carry information
    necessary to better control the MTU, e.g., through a SCADA Server.
A python implementation of all can be found in `messages.py` 
Every message includes an `id` field to easily differentiate the types of messsages.
Currently, all data is forwarded per-packet and not further aggregated.
All messages will include a `reference_nr` field, set by the MTU.
For more explanation of it, see [###Reference Numbers].
All keys in JSON are strings! 
We give here the expected mappings in JSON:

#### Queueing, Collisions, and Retries
If a datapoint is considered *active*, e.g., did not reply to a C_SC yet, the MTU will reject
a command by default. Setting a message's `queue_on_collision` to `True` will queue the command instead.
In either case, a corresponding `Confirmation` message is send back stating the decision.

In case a failure occured while performing a command, e.g., when an IOs quality is bad upon RTU reception,
 it can be retried if the message's `max_tries`
are set as `> 1`.

#### Reference Numbers
Only the *very first* control message will be published in full, stating the corresponding reference_nr.
*All* other future messages that stand in direct contact with it are either
- Monitoring messages containing IOs
- a `Confirmation` message.

Either will include the precursing message's reference_nr as its reference_nr.

#### ProcessInfoMonitoring:
   for type-IDs 1-40, containing values for specific IOAs:.
   Purely MTU -> Subscriber direction.
   
   
        - id -> PROCESS_INFO_MONITORING
        - coa -> [int/str]
        - val_map -> JSON-Dict[int/str -> [float, int, bool, Tuple]]
            - contains the returned IOs as IOA-value map
            - JSON-List in case a timestamp is returned as well
        - type_ID -> [int]
        - cot -> [int]
        - reference_nr -> [str]

#### ProcessInfoControl:
For Process information msgs in control direction. (TypeID 45-64)
   All IOs reference to the same COA. Not yet supported by the MTU.
   @Lennart:
    The current implementation would actually also allow for multi-coa messages via {coa -> {ioa -> target_val}} structs
                 
        - id -> PROCESS_INFO_CONTROL
        - coa -> [int/str]
        - type_ID -> [int]
        - val_map -> JSON-Dict[int/str -> [float, int, bool]]
        - reference_nr -> [str] 
        - max_tries -> [int] = 1
        - queue_on_collision -> [bool] = False
        - cot [int] = ACTIVATION
            
#### Confirmation:
Signals status of a command. `reference_nr` is the corresponding nr of the message it refers to.


        - id -> CONF
        - result -> JSON-Dict[str, str]
        - reference_nr -> [int]
        - max_tries -> [int]
        
The *result* attribute the following use:

- status -> {"Send", "Successful Confirmation", "Successful Termination", "Failed", "Queued"}
- reason -> x ; Used for all types of failed statuss'
-
-

The *max_tries* will be the original's message value, decremented by one for each try until 0.


/#### Change Periodicity:
/Command to swap the RTU supplying constant updates for an IO. type-ID: 113. Not yet supported.
/
/        id -> PERIODIC
/        target_coa -> [int/str]
/        target_ioa -> [int/str]
/        reference_nr -> [int]
 
#### ParameterControl:
Change Parameters on a field device/ RTU. Type-IDs: 110-113. Not yet supported.

    id -> PARAMETER_CONTROL
    coa -> [int/str]
    ioa -> [int/str]
    value -> [bool, int, float]
    type_ID -> [int]
    reference_nr -> [int]
    max_tries -> [int] = 1
    queued_on_collision -> [bool] = False

#### SysInfoControl:
General commands (typeID 100 - 107). Not yet supported by the server.

        - id -> SYS_INFO_REQ
        - type_ID -> [int]
        

#### FileTransferReq
General file transfer requests. Type-IDs: 122, 124, 125, 126, 127.
Not yet supported.

        - id -> FILE_TRANSFER_REQ
        ... Yet unspecified

#### FileTransferReply
Type IDs: 120, 121, 123. Not yet supported on the server side.

        - id -> FILE_TRANSFER_REPLY
        ... Yet unspecified
        
        
#### Total Interrogation Request:
Request all information of possible relevance (Datapoints, RTU IPs, ...).

        - id -> TOTAL_INTERRO_REQ
        
#### Total Interrogation Reply:
Currently: Sending RTU status and datapoints available to MTU.
Likely adding further information in the future

    - id -> TOTAL_INTERRO_REPLY
    - status -> JSON-Dict[str, JSON-List[int, str, int]] (RTU-COA -> [RTU_COA, IP, status)
    - datapoints -> JSON-Dict[str, JSON-Dict[str, JSON-List[int, int, int, int, str, Unionp[str, bool]]]]
        Format: COA -> IOA -> (COA, IOA, type_ID, cot, relation, writeable); 
            - currently: writable = 'unknown' if type_ID not in (45, 70)
            
#### RTU Status Request:
Solely request connection status for all RTUs:
    - id -> RTU_STATUS_REQ
    
#### RTU Status Reply:
Return Connection Status:

    - closed = 0
    - open = 1
    - interro_started = 2
    - interro_done = 3
 
 as message with:
 
    - id -> RTU_STATUS_REPLY
    - status -> JSON-Dict[str (COA), JSON-List[int (COA), str (IP), int (status)]]    

### Reference Numbers


### Constants

#### Message IDs (IntEnum MsgID)
- PROCESS_INFO_MONITORING = 1
- PROCESS_INFO_CONTROL = 2
- PARAMETER_CONTROL = 3
- SYS_INFO_CONTROL = 5
- SYS_INFO_MONITORING = 6
- FILE_TRANSFER_REQ = 7
- FILE_TRANSFER_REPLY = 8
- CONF = 9
- TOTAL_INTERRO_REQ = 11
- TOTAL_INTERRO_REPLY = 12
- RTU_STATUS_REQ = 13
- RTU_STATUS_REPLY = 14
- READ_DATAPOINT = 16
- PERIODIC_UPDATE = 17

#### Other Default Values:
- UNSET_REFERENCE_NR = -1
- NO_TS = -2
