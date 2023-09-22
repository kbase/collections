from src.common.collection_column_specs import load_specs
from src.common.product_models.columnar_attribs_common_models import (
    AttributesColumnSpec,
    FilterStrategy,
    ColumnarAttributesSpec,
    ColumnType,
)
from pathlib import Path
from pytest import raises
import re


# TODO TEST setup more test spec files and do more comprehensive tests checking the entire
#           spec structure. Allow setting the spec directory to a test dir


def test_get_collections_for_dataproduct():
    assert load_specs.get_collections_for_data_product("genome_attribs"
        ) == {'PMI', 'GROW', 'ENIGMA', 'GTDB'}


def test_all_specs_load_singular():
    # just check the specs' format is ok and common vars
    
    st = ColumnType.STRING
    ident = FilterStrategy.IDENTITY
    
    files = list(Path(load_specs.__file__).parent.glob("*.yml"))
    if not files:
        assert 0, "No spec files found"
    for f in files:
        print(f"Checking spec {f}")
        data_product, collection = str(f).split('-')
        collection = collection[:-4]
        spec = load_specs.load_spec(data_product, collection)
        key2spec = {c.key: c for c in spec.columns}
        assert key2spec["coll"] == AttributesColumnSpec(
            key="coll", type=st, filter_strategy=ident)
        assert key2spec["load_ver"] == AttributesColumnSpec(
            key="load_ver", type=st, filter_strategy=ident)
        


def test_all_specs_load_merge():
    st = ColumnType.STRING
    ft = ColumnType.FLOAT
    it = ColumnType.INT
    
    ident = FilterStrategy.IDENTITY
    ftext = FilterStrategy.FULL_TEXT
    inar = FilterStrategy.IN_ARRAY
    
    spec = load_specs.load_spec("genome_attribs")
    # just check a few fields
    key2spec = {c.key: c for c in spec.columns}
    assert key2spec["kbase_id"] == AttributesColumnSpec(
        key="kbase_id", type=st, filter_strategy=ident)
    assert key2spec["classification"] == AttributesColumnSpec(
            key="classification", type=st, filter_strategy=ftext)
    assert key2spec["Contamination"] == AttributesColumnSpec(key="Contamination", type=ft)
    assert key2spec["checkm_contamination"] == AttributesColumnSpec(
            key="checkm_contamination", type=ft)
    assert key2spec["translation_table"] == AttributesColumnSpec(
        key="translation_table", type=it)
    assert key2spec["_mtchsel"] == AttributesColumnSpec(
        key="_mtchsel", type=st, filter_strategy=inar)


def test_load_single_spec_from_toolchain():
    st = ColumnType.STRING
    ft = ColumnType.FLOAT
    it = ColumnType.INT
    
    ident = FilterStrategy.IDENTITY
    ftext = FilterStrategy.FULL_TEXT
    for col in ["GROW", "PMI", "ENIGMA"]:
        spec = load_specs.load_spec("genome_attribs", col)
        
        assert type(spec) == ColumnarAttributesSpec
        # just check a few fields
        key2spec = {c.key: c for c in spec.columns}
        assert key2spec["kbase_id"] == AttributesColumnSpec(
            key="kbase_id", type=st, filter_strategy=ident)
        assert key2spec["Contamination"] == AttributesColumnSpec(key="Contamination", type=ft)
        assert key2spec["classification"] == AttributesColumnSpec(
            key="classification", type=st, filter_strategy=ftext)
        assert key2spec["translation_table"] == AttributesColumnSpec(
            key="translation_table", type=it)


def test_load_key_collision():
    err = "Column spec conflict for data product test_dp, collections COL1 and COL2 on key bar"
    with raises(ValueError, match=f"^{re.escape(err)}$"):
        load_specs.load_spec("test_dp")
