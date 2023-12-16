"""
Common pydantic models for heat map data products.
"""

from enum import Enum
from typing import Annotated, Any

from pydantic import BaseModel, Field

from src.common.product_models.columnar_attribs_common_models import ColumnType as AttribsColumnType
from src.common.product_models.common_models import SubsetProcessStates

# these fields need to match the fields in the models below.
FIELD_HEATMAP_DATA = "data"
FIELD_HEATMAP_MIN_VALUE = "min_value"
FIELD_HEATMAP_MAX_VALUE = "max_value"
FIELD_HEATMAP_COUNT = "count"
FIELD_HEATMAP_VALUES = 'values'
FIELD_HEATMAP_ROW_CELLS = "cells"
FIELD_HEATMAP_ROW_META = "meta"
FIELD_HEATMAP_CELL_ID = 'cell_id'
FIELD_HEATMAP_COL_ID = 'col_id'
FIELD_HEATMAP_CELL_VALUE = "val"
FIELD_HEATMAP_NAME = "name"
FIELD_HEATMAP_DESCR = "description"
FIELD_HEATMAP_TYPE = "type"
FIELD_HEATMAP_CATEGORY = "category"
FIELD_HEATMAP_COLUMNS = "columns"
FIELD_HEATMAP_CATEGORIES = "categories"
FIELD_HEATMAP_CELL_DETAIL_ENTRY_VALUE = "val"
FIELD_HEATMAP_CELL_DETAIL_ENTRY_ID = "id"

_FLD_CELL_ID = Field(
    example="4",
    description="The unique ID of the cell in the heatmap."
)

HEATMAP_COL_PREFIX = "col"
HEATMAP_COL_SEPARATOR = '_'


class ColumnType(str, Enum):
    """
    The type of a column's values.
    """
    FLOAT = "float",
    """ A float. """
    INT = "int"
    """ An integer. """
    COUNT = "count"
    """ Similar to an integer, but represents a numeric count. """
    BOOL = "bool"
    """ A boolean. """


def trans_column_type_heatmap_to_attribs(col_type: ColumnType) -> AttribsColumnType:
    """
    Translate a heatmap column type to an attributes column type.
    This method is suitable for models like filters that exclusively operate on attributes column types

    col_type: the heatmap column type
    """
    heatmap_to_attribs_mapping = {
        ColumnType.FLOAT: AttribsColumnType.FLOAT,
        ColumnType.INT: AttribsColumnType.INT,
        ColumnType.COUNT: AttribsColumnType.INT,
        ColumnType.BOOL: AttribsColumnType.BOOL
    }

    if col_type not in heatmap_to_attribs_mapping:
        raise ValueError(f'column type {col_type} is not supported by the heatmap')

    return heatmap_to_attribs_mapping[col_type]


class ColumnInformation(BaseModel):
    """
    Information about a column in the heat map, e.g. its name, ID, description, etc.
    """
    col_id: str = Field(
        example="4",
        description="An opaque ID for the column, typically much shorter than the name"
    )
    name: str = Field(
        example="Spizizen minimal media",
        description="The name of the column",
    )
    description: str = Field(
        example="Spizizen medium (SM) is a popular minimal medium for the cultivation of "
                + "B. subtilis.",
        description="The description of the column."
    )
    type: ColumnType = Field(
        example=ColumnType.COUNT.value,
        description="The type of the column values."
    )


class ColumnCategory(BaseModel):
    """
    A set of columns grouped into a disjoint, non-hierarchical category.
    """
    category: str | None = Field(
        example="Minimal media",
        description="The name of the category that groups columns together. Null if "
                    + "columns are not categorized."
    )
    columns: list[ColumnInformation] = Field(
        description="The columns in the category, provided in render order."
    )


class HeatMapMeta(BaseModel):
    """
    Provides meta information about the data in a heatmap.
    """
    categories: list[ColumnCategory] = Field(
        description="The categories in the heat map, provided in render order."
    )
    min_value: float = Field(
        example=2.56,
        description="The minimum value of the heat map data in the entire data set."
    )
    max_value: float = Field(
        example=42.0,
        description="The maximum value of the heat map data in the entire data set."
    )
    count: Annotated[int, Field(
        example=3,
        description="The number of rows in the heatmap."
    )]


class Cell(BaseModel):
    """
    Information about an individual cell in a heatmap.
    """
    cell_id: str = _FLD_CELL_ID
    col_id: str = Field(
        example="8",
        description="The ID of the column in which this cell is located."
    )
    val: float | int | bool = Field(
        example=4.2,
        description="The value of the heatmap at this cell."
    )


class HeatMapRow(BaseModel):
    """
    A row of cells in a heatmap.
    """
    match: bool | None = Field(
        description="True if this row is included in a match. Null if there is no match."
    )
    sel: bool | None = Field(
        description="True if this row is included in a selection. Null if there is no selection."
    )
    kbase_id: str = Field(
        example="GB_GCA_000006155.2",
        description="The unique ID of the subject of a heatmap row. Often a genome, MAG, etc."
    )
    kbase_display_name: str | None = Field(
        example="altamaha_2019_sw_WHONDRS-S19S_0010_A_bin_34_mag_assembly",
        description="The name shown for the subject in a heatmap row, typically an KBase object name."
    )
    cells: list[Cell] = Field(
        description="The cells in the row of the heatmap in render order."
    )
    meta: dict[str, str] | None = Field(
        example={"growth_media": "Spizizen minimal media + 0.5mM biotin"},
        description="Arbitrary metadata about the data in the row"
    )


class HeatMap(SubsetProcessStates):
    """
    A heatmap or a portion of a heatmap.

    If match or selection IDs are supplied, their processing states are returned.

    Additionally either data, min_value, and max_value, or count may be supplied.
    """
    data: list[HeatMapRow] | None = Field(
        description="The rows in the heatmap."
    )
    min_value: float | None = Field(
        example=32.4,
        description="The minimum cell value in the rows in this heatmap "
                    + "or null if there are no rows."
    )
    max_value: float | None = Field(
        example=71.8,
        description="The maximum cell value in the rows in this heatmap "
                    + "or null if there are no rows."
    )
    count: int | None = Field(
        example=42,
        description="The total number of rows that match the query."
    )


class CellDetailEntry(BaseModel):
    """
    An entry in a list of cell detail values.
    """
    id: str = Field(
        example="spo0A",
        description="The ID of of the cell entry, often a gene name."
    )
    val: float | bool = Field(
        example=56.1,
        description="The value of the cell entry."
    )


class CellDetail(BaseModel):
    """
    Detailed information about a cell in a heatmap.
    """
    cell_id: str = _FLD_CELL_ID
    values: list[CellDetailEntry]


def transform_heatmap_row_cells(data: dict[str, Any]):
    """
    Transform, in place, the cells structure in a heatmap row to a new structure.

    The new structure is a set of keys and values where the keys are constructed from the old structure.
    new structured key format: <HEATMAP_COL_PREFIX>_<col_id>_<FIELD_HEATMAP_CELL_ID|FIELD_HEATMAP_CELL_VALUEl>

    e.g.

    The old structure:
    "cells": [
        {
            "cell_id": "cell_0",
            "col_id": "0",
            "val": 0.0
        },
        {
            "cell_id": "cell_1",
            "col_id": "1",
            "val": 1.0
        }
    ]

    The new structure:
    "col_0_cell_id": "cell_0",
    "col_0_val": 0.0,
    "col_1_cell_id": "cell_1",
    "col_1_val": 1.0

    """

    # Iterate over the 'cells' structure and remove them from the data structure while constructing the new structure
    for cell in data.pop(FIELD_HEATMAP_ROW_CELLS):
        col_id = cell[FIELD_HEATMAP_COL_ID]

        # Construct keys and values for the new structure
        cell_id_key = f"{HEATMAP_COL_PREFIX}{HEATMAP_COL_SEPARATOR}{col_id}{HEATMAP_COL_SEPARATOR}{FIELD_HEATMAP_CELL_ID}"
        cell_val_key = f"{HEATMAP_COL_PREFIX}{HEATMAP_COL_SEPARATOR}{col_id}{HEATMAP_COL_SEPARATOR}{FIELD_HEATMAP_CELL_VALUE}"

        # Add the new keys and values to the data structure
        data[cell_id_key] = cell[FIELD_HEATMAP_CELL_ID]
        data[cell_val_key] = cell[FIELD_HEATMAP_CELL_VALUE]


def revert_transformed_heatmap_row_cells(data: dict[str, Any]):
    """
    Revert, in place, the transformation of heatmap row cells.
    Input key format: <HEATMAP_COL_PREFIX>_<col_id>_<FIELD_HEATMAP_CELL_ID|FIELD_HEATMAP_CELL_VALUEl>

    The structure (input) to be reverted:
    "col_0_cell_id": "cell_0",
    "col_0_val": 0.0,
    "col_1_cell_id": "cell_1",
    "col_1_val": 1.0

    The resulting structure:
    "cells": [
        {
            "cell_id": "cell_0",
            "col_id": "0",
            "val": 0.0
        },
        {
            "cell_id": "cell_1",
            "col_id": "1",
            "val": 1.0
        }
    ]
    """

    # Get the keys that need to be reconstructed
    keys_to_remove = [key for key in data.keys() if key.startswith(HEATMAP_COL_PREFIX) and
                      (key.endswith(FIELD_HEATMAP_CELL_ID) or key.endswith(FIELD_HEATMAP_CELL_VALUE))]

    cells_dict = {}
    for key in keys_to_remove:
        parts = key.split(HEATMAP_COL_SEPARATOR)
        col_id = parts[1]
        col_type = HEATMAP_COL_SEPARATOR.join(parts[2:])
        if not col_id.isdigit():
            raise ValueError(f"Column ID '{col_id}' is not an integer.")

        if col_type not in [FIELD_HEATMAP_CELL_ID, FIELD_HEATMAP_CELL_VALUE]:
            raise ValueError(f'Unexpected column type: {col_type}')

        cells_dict.setdefault(col_id, {})
        cells_dict[col_id][col_type] = data[key]

    cells = [{
        FIELD_HEATMAP_COL_ID: col_id,
        FIELD_HEATMAP_CELL_ID: cell_data[FIELD_HEATMAP_CELL_ID],
        FIELD_HEATMAP_CELL_VALUE: cell_data[FIELD_HEATMAP_CELL_VALUE]
    } for col_id, cell_data in cells_dict.items()]

    data[FIELD_HEATMAP_ROW_CELLS] = cells

    for key in keys_to_remove:
        data.pop(key)
