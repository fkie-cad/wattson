
class Function:
    def __init__(self, options):
        self.options = options
        self.intervals = options["intervals"]
        self.direction = options["direction"] if "direction" in options else None
        self.value = options["values"] if "values" in options else None
        self.diff_since_start = options["diff"]
        self.reference_value = options["reference_value"]
        self.logger = options["logger"]

    def handles_value_type(self, value_type) -> bool:
        pass

    def apply(self):
        pass

    def _apply_direction(self, original_value, temp_value):
        if self.direction == "up":
            if temp_value < 0:
                temp_value *= -1
            return original_value + temp_value
        elif self.direction == "down":
            if temp_value < 0:
                temp_value *= -1
            return original_value - temp_value
        elif self.direction == "custom":
            return self.options["func"](self.options["func_options"])
        else:
            self.logger.error(f"Invalid direction: {self.direction}")
            return original_value
