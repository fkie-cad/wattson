def translate_value(value, measure):
    if measure in ["p_mw", "q_mvar", "kv", "ka"]:
        value, unit = extract_unit(value)
        unit = unit.lower()
        prefix = ""
        if len(unit) > 1:
            prefix = unit[0]

        # Scale value to Mega X
        if prefix == "":
            value = value / 1000000
        if prefix == "k":
            value = value / 1000
        if prefix == "m":
            pass

        if measure in ["kv", "ka"]:
            value = value * 1000
        return value
    return value


def extract_unit(value):
    numeric = '0123456789-.'
    i = 0
    for i, c in enumerate(value):
        if c not in numeric:
            break
    number = float(value[:i])
    unit = value[i:].lstrip()
    return number, unit
