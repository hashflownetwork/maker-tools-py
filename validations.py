import re

def validate_maker_name(name: str) -> None:
    if not re.match(r'^mm[0-9]+$', name):
        raise ValueError(f"Maker name must be external name of format 'mm123'. Got '{name}'")