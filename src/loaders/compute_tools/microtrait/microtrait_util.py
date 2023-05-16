import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from src.loaders.common.loader_common_names import SYS_TRAIT_ID, UNWRAPPED_GENE_COL

# column names from the rule2trait file that contain the trait names
_RULE2TRAIT_TRAIT_NAME_COLS = ['microtrait_trait-name1', 'microtrait_trait-name2', 'microtrait_trait-name3']
_RULE2TRAIT_RULE_NAME_COL = 'microtrait_rule-name'  # column name from the rule2trait file that contains the rule name
_RULE2TRAIT_RULE_SUBSTRATE_COL = 'microtrait_rule-substrate'

_SUBSTRATE2RULE_SUBSTRATE_NAME_COL = 'microtrait_substrate-name'  # column name from the substrate2rule_file file that contains the substrate name

# `microtrait_ruleunwrapped.txt` file unfortunately has no header, so we need to specify the column names
_UNWRAPPED_RULE_COL = 'unwrapped_rule'
_RULEUNWRAPPED_RULE_NAME_COL = 'rule_name'
_RULEUNWRAPPED_COLS = [
    _RULEUNWRAPPED_RULE_NAME_COL,
    'rule_boolean',
    'rule_display_name',
    _UNWRAPPED_RULE_COL]


def _retrieve_trait_substrate_mapping(
        substrate2rule_file: Path
) -> dict[str, set[str]]:
    # retrieve the trait to substrate mapping from the substrate2rule file
    substrate2rule_df = pd.read_csv(substrate2rule_file, sep='\t')

    # create a dictionary of trait names to substrate names from the substrate2rule file
    trait_substrate_mapping = defaultdict(set)
    for index, row in substrate2rule_df.iterrows():
        substrate_name = row[_SUBSTRATE2RULE_SUBSTRATE_NAME_COL]
        for trait_name_col in _RULE2TRAIT_TRAIT_NAME_COLS:
            trait_name = row[trait_name_col]
            if pd.notna(trait_name):
                trait_substrate_mapping[trait_name].add(substrate_name)
            else:
                raise ValueError(f"Trait name is null for row {index} in {substrate2rule_file}")

    return trait_substrate_mapping


def _retrieve_genes(
        unwrapped_rule: str
) -> set[str]:
    # retrieve and extract gene names from the unwrapped rule expression.
    # i.e. "('pcbA' | 'pcbB' | 'pcbC' | 'pcbD' | 'pcbE' | 'pcbF' | 'pcbG' | 'pcbH')", "((((((('tc.fev')))))))",
    # "(('sreA' & 'sreB' & 'sreC') | ('hydG' & 'hydB_4'))", etc.

    # find gene names between single quotes
    gene_names = set(re.findall(r"'([^']+)'", unwrapped_rule))

    return gene_names


def _write_trait_unwrapped_genes(
        trait_unwrapped_rules_file: Path,
        trait_rule_unwrapped_mapping: dict[str, set[str]]
) -> None:
    # write the trait to unwrapped rule mapping to a file
    with open(trait_unwrapped_rules_file, 'w') as file:
        file.write(f"{SYS_TRAIT_ID}\t{UNWRAPPED_GENE_COL}\n")
        for trait, rule_set in trait_rule_unwrapped_mapping.items():
            # Join the set elements into a single string
            unwrapped_rules = ';'.join(rule_set)
            file.write(f"{trait}\t{unwrapped_rules}\n")


def _create_trait_unwrapped_genes_mapping(
        trait_rule_unwrapped_mapping: dict[str, set[str]],
        rule_dict: dict[str, str]
) -> dict[str, set[str]]:
    # create a dictionary of trait names to unwrapped gene names
    trait_unwrapped_genes_mapping = defaultdict(set)
    for trait_name, rule_names in trait_rule_unwrapped_mapping.items():
        for rule_name in rule_names.copy():
            unwrapped_rule = rule_dict.get(rule_name)
            if unwrapped_rule:
                gene_names = _retrieve_genes(unwrapped_rule)
                trait_unwrapped_genes_mapping[trait_name].update(gene_names)
            else:
                raise ValueError(f"Rule name {rule_name} not found in ruleunwrapped file")

    keys_with_empty_set = [k for k, v in trait_unwrapped_genes_mapping.items() if len(v) == 0]
    if keys_with_empty_set:
        raise ValueError(f"Substrate names not found in substrate2rule file for traits: {keys_with_empty_set}")

    return trait_unwrapped_genes_mapping


def create_trait_unwrapped_genes(
        rule2trait_file: Path,
        ruleunwrapped_file: Path,
        substrate2rule_file: Path,
        trait_unwrapped_genes_file: Path = None,
) -> dict[str, set[str]]:
    """
    Generate a mapping of traits to associated unwrapped genes.

    """
    trait_rule_unwrapped_mapping = defaultdict(set)

    # read the rule2trait file and populate rule names to each trait
    rule2trait_df = pd.read_csv(rule2trait_file, sep='\t')
    for index, row in rule2trait_df.iterrows():
        rule_name = row[_RULE2TRAIT_RULE_NAME_COL]
        for trait_name_col in _RULE2TRAIT_TRAIT_NAME_COLS:
            trait_name = row[trait_name_col]
            trait_rule_unwrapped_mapping[trait_name].add(rule_name) if pd.notna(trait_name) else None

    trait_substrate_mapping = _retrieve_trait_substrate_mapping(substrate2rule_file)

    # split the rule substrate column into an array of substrates
    # i.e. "trehalose;maltose;sucrose" -> ["trehalose", "maltose", "sucrose"]
    rule_substrate_array = rule2trait_df[_RULE2TRAIT_RULE_SUBSTRATE_COL].str.split(';')
    # fill the NaN values with empty lists (some rules have NA value for substrate)
    rule_substrate_array = rule_substrate_array.fillna(value=pd.Series([[]] * len(rule_substrate_array)))

    # find the rule names associated with the substrate names from the rule2trait file
    for trait_name, substrate_names in trait_substrate_mapping.items():
        for substrate_name in substrate_names:
            substrate_present = rule_substrate_array.apply(lambda arr: substrate_name in arr)
            rule_names = rule2trait_df[substrate_present][_RULE2TRAIT_RULE_NAME_COL].unique()
            trait_rule_unwrapped_mapping[trait_name].update(set(rule_names))

    # read the ruleunwrapped file and populate unwrapped rule expressions to each trait
    ruleunwrapped_df = pd.read_csv(ruleunwrapped_file, sep='\t', names=_RULEUNWRAPPED_COLS)
    # create a dictionary of rule names to unwrapped rule expressions
    rule_dict = dict(zip(ruleunwrapped_df[_RULEUNWRAPPED_RULE_NAME_COL], ruleunwrapped_df[_UNWRAPPED_RULE_COL]))

    trait_unwrapped_genes_mapping = _create_trait_unwrapped_genes_mapping(trait_rule_unwrapped_mapping, rule_dict)

    if trait_unwrapped_genes_file is not None:
        _write_trait_unwrapped_genes(trait_unwrapped_genes_file, trait_unwrapped_genes_mapping)

    return trait_unwrapped_genes_mapping
