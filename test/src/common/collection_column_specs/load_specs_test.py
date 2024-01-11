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
        data_product, collection = f.name.split('-')
        collection = collection[:-4]
        spec = load_specs.load_spec(data_product, collection)
        assert len(spec.spec_files) == 1
        assert spec.spec_files[0].name == f"{data_product}-{collection}.yml"
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
    ngram = FilterStrategy.NGRAM

    spec = load_specs.load_spec("genome_attribs")
    assert {f.name for f in spec.spec_files} == {
        f"genome_attribs-{col}.yml" for col in ["ENIGMA", "PMI", "GTDB", "GROW"]
    }
    # just check a few fields
    key2spec = {c.key: c for c in spec.columns}
    assert key2spec["kbase_id"] == AttributesColumnSpec(
        key="kbase_id", type=st, filter_strategy=ident, display_name="KBase ID", category="Identifiers",)
    assert key2spec["classification"] == AttributesColumnSpec(
        key="classification", type=st, filter_strategy=ngram, display_name="Classification", category="Taxonomy",)
    assert key2spec["Contamination"] == AttributesColumnSpec(
        key="Contamination", type=ft, display_name="CheckM Contamination", category="Quality",)
    assert key2spec["checkm_contamination"] == AttributesColumnSpec(
        key="checkm_contamination", type=ft, display_name="CheckM Contamination", category="Quality",)
    assert key2spec["translation_table"] == AttributesColumnSpec(
        key="translation_table", type=it, display_name="Translation Table", category="Other",)
    assert key2spec["_mtchsel"] == AttributesColumnSpec(
        key="_mtchsel", type=st, filter_strategy=ident)


def test_load_single_spec_from_toolchain():
    st = ColumnType.STRING
    ft = ColumnType.FLOAT
    it = ColumnType.INT
    
    ident = FilterStrategy.IDENTITY
    ngram = FilterStrategy.NGRAM
    
    for col in ["GROW", "PMI", "ENIGMA"]:
        spec = load_specs.load_spec("genome_attribs", col)
        
        assert type(spec) == ColumnarAttributesSpec
        # just check a few fields
        key2spec = {c.key: c for c in spec.columns}
        assert key2spec["kbase_id"] == AttributesColumnSpec(
            key="kbase_id", type=st, filter_strategy=ident, display_name="KBase ID", category="Identifiers", )
        assert key2spec["Contamination"] == AttributesColumnSpec(
            key="Contamination", type=ft, display_name="CheckM Contamination", category="Quality",)
        assert key2spec["classification"] == AttributesColumnSpec(
            key="classification", type=st, filter_strategy=ngram, display_name="Classification", category="Taxonomy", )
        assert key2spec["translation_table"] == AttributesColumnSpec(
            key="translation_table", type=it, display_name="Translation Table", category="Other", )


def test_no_specs():
    err = "No specs found for data product nospecshere"
    with raises(ValueError, match=f"^{re.escape(err)}$"):
        load_specs.load_spec("nospecshere")


def test_load_key_collision():
    err = "Column spec conflict for data product test_dp, collections COL1 and COL2 on key bar"
    with raises(ValueError, match=f"^{re.escape(err)}$"):
        load_specs.load_spec("test_dp")
