from src.common.collection_column_specs import load_specs
from src.common.product_models.columnar_attribs_common_models import (
    ColumnarAttributesSpec,
    AttributesColumnSpec,
    ColumnType,
    FilterStrategy
)
from pathlib import Path


def test_all_specs_load():
    # just check the specs' format is ok
    files = list(Path(load_specs.__file__).parent.glob("*.yml"))
    if not files:
        assert 0, "No spec files found"
    for f in files:
        print(f"Checking spec {f}")
        data_product, collection = str(f).split('-')
        collection = collection[:-4]
        load_specs.load_spec(data_product, collection)


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
        assert spec.columns[0] == AttributesColumnSpec(key="kbase_id", type=st, filter_strategy=ident)
        key2spec = {c.key: c for c in spec.columns}
        assert key2spec["Contamination"] == AttributesColumnSpec(key="Contamination", type=ft)
        assert key2spec["classification"] == AttributesColumnSpec(
            key="classification", type=st, filter_strategy=ftext)
        assert key2spec["translation_table"] == AttributesColumnSpec(key="translation_table", type=it)
