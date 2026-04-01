from src.datamart.mapping import get_mapping, ALL_MAPPINGS


def test_get_mapping_returns_list():
    result = get_mapping("orders")
    assert isinstance(result, list)
    assert len(result) > 0


def test_all_mappings_have_required_keys():
    required_keys = {"source_col", "target_col", "data_type",
                     "transform", "nullable", "description"}
    for table, mappings in ALL_MAPPINGS.items():
        for entry in mappings:
            missing = required_keys - set(entry.keys())
            assert not missing, f"Table '{table}' mapping entry missing keys: {missing}"


def test_non_nullable_fields_have_source_col():
    for table, mappings in ALL_MAPPINGS.items():
        for entry in mappings:
            if not entry["nullable"] and entry["transform"] != "derived":
                assert entry["source_col"] is not None, \
                    f"Non-nullable field '{entry['target_col']}' in '{table}' has no source column"


def test_get_mapping_raises_for_unknown_table():
    import pytest
    with pytest.raises(KeyError, match="No mapping found"):
        get_mapping("nonexistent_table")


def test_all_five_tables_defined():
    expected = {"orders", "customers", "order_items", "reviews", "exchange_rates"}
    assert expected == set(ALL_MAPPINGS.keys())