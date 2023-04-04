"""
Common pydantic and fastAPI models for heat map data products.
"""

from pydantic import BaseModel, Field


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


class Columns(BaseModel):
    """
    Provides information about the set of columns in a heatmap view.
    """
    categories: list[ColumnCategory] = Field(
        description="The categories in the heat map, provided in render order."
    )
