# Create Suricata rules

1. Configure rule, e.g.
alert tcp any any -> any any (msg:"ET SCADA IEC-104 I-Format"; content:"|68|"; offset:0; depth:1; byte_test:2,=,0x00,2,bitmask 00000001; sid:1000020; rev:2;)
2. Append rule(s) to /var/lib/suricata/rules/suricata.rules
3. (Re)Start Suricata

## In the wattson-ids container

Go to `edit_config_files.py` and add the rule to the rules at the top of the file.