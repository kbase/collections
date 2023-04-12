"""
Common pydantic and fastAPI models for heat map data products.
"""

from pydantic import BaseModel, Field

from src.service import models


_FLD_CELL_ID = Field(
    example="4",
    description="The unique ID of the cell in the heatmap."
)


class ColumnInformation(BaseModel):
    """
    Information about a column in the heat map, e.g. its name, ID, description, etc.
    """
    id: str = Field(
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
        descrption="The columns in the category, provided in render order."
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


class Cell(BaseModel):
    """
    Information about an indvidual cell in a heatmap.
    """
    celid: str = _FLD_CELL_ID
    colid: str = Field(
        example="8",
        description="The ID of the column in which this cell is located."
    )
    val: float = Field(
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
    cells: list[Cell] = Field(
        description="The cells in the row of the heatmap in render order."
    )


class HeatMap(BaseModel):
    """
    A heatmap or a portion of a heatmap.

    If match or selection IDs are supplied, their processing states are returned.

    Additionally either data, min_value, and max_value, or count may be supplied.
    """
    heatmap_match_state: models.ProcessState | None = Field(
        example=models.ProcessState.PROCESSING,
        description="The processing state of the match (if any) for this data product. "
            + "This data product requires additional processing beyond the primary match."
    )
    heatmap_selection_state: models.ProcessState | None = Field(
        example=models.ProcessState.FAILED,
        description="The processing state of the selection (if any) for this data product. "
            + "This data product requires additional processing beyond the primary selection."
    )
    data: list[HeatMapRow] | None = Field(
        description="The rows in the heatmap."
    )
    min_value: float | None = Field(
        example=32.4,
        description="The minimum cell value in the row in this heatmap "
            + "or null if there are no rows."
    )
    max_value: float | None = Field(
        example=71.8,
        description="The maximum cell value in the row in this heatmap "
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
    value: float | bool = Field(
        example=56.1,
        description="The value of the cell entry."
    )
    class Config:
        smart_union=True


class CellDetail(BaseModel):
    """
    Detailed information about a cell in a heatmap.
    """
    celid: str = _FLD_CELL_ID
    values: list[CellDetailEntry]
