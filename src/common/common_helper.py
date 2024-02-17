"""
This module contains common helper functions.

"""

from typing import Any


def obj_info_to_upa(obj_info: list[Any], underscore_sep=False) -> str:
    """
    Convert workspace object info to UPA.

    obj_info - the object info list
    underscore_sep - whether to use underscore as separator (default: False)
    """
    sep = "_" if underscore_sep else "/"

    return f"{obj_info[6]}{sep}{obj_info[0]}{sep}{obj_info[4]}"