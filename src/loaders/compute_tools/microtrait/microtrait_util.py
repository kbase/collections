import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from src.loaders.common.loader_common_names import SYS_TRAIT_ID

# column names from the rule2trait file that contain the trait names
_RULE2TRAIT_TRAIT_NAME_COLS = ['microtrait_trait-name1', 'microtrait_trait-name2', 'microtrait_trait-name3']
_RULE2TRAIT_RULE_NAME_COL = 'microtrait_rule-name'  # column name from the rule2trait file that contains the rule name

# `microtrait_ruleunwrapped.txt` file unfortunately has no header, so we need to specify the column names
_UNWRAPPED_RULE_COL = 'unwrapped_rule'
_UNWRAPPED_GENE_COL = 'unwrapped_genes'  # column name that contains parsed gene name from unwrapped rule
_RULEUNWRAPPED_RULE_NAME_COL = 'rule_name'
_RULEUNWRAPPED_COLS = [
    _RULEUNWRAPPED_RULE_NAME_COL,
    'rule_boolean',
    'rule_display_name',
    _UNWRAPPED_RULE_COL]


def create_trait_unwrapped_rules(
        rule2trait_file: Path,
        ruleunwrapped_file: Path,
        trait_unwrapped_rules_file: Path = None,
) -> dict[str, set[str]]:
    """
    Generate a mapping of traits to unwrapped rule expressions.

    """
    trait_rule_unwrapped_mapping = defaultdict(set)

    # read the rule2trait file and populate rule names to each trait
    rule2trait_df = pd.read_csv(rule2trait_file, sep='\t')
    for index, row in rule2trait_df.iterrows():
        rule_name = row[_RULE2TRAIT_RULE_NAME_COL]
        for trait_name_col in _RULE2TRAIT_TRAIT_NAME_COLS:
            trait_name = row[trait_name_col]
            trait_rule_unwrapped_mapping[trait_name].add(rule_name) if pd.notna(trait_name) else None

    # read the ruleunwrapped file and populate unwrapped rule expressions to each trait
    ruleunwrapped_df = pd.read_csv(ruleunwrapped_file, sep='\t', names=_RULEUNWRAPPED_COLS)
    # create a dictionary of rule names to unwrapped rule expressions
    rule_dict = dict(zip(ruleunwrapped_df[_RULEUNWRAPPED_RULE_NAME_COL], ruleunwrapped_df[_UNWRAPPED_RULE_COL]))

    for trait_name, rule_names in trait_rule_unwrapped_mapping.items():
        for rule_name in rule_names.copy():
            unwrapped_rule = rule_dict.get(rule_name)
            if unwrapped_rule:
                # find and extract gene names from the unwrapped rule expression.
                # i.e. "('pcbA' | 'pcbB' | 'pcbC' | 'pcbD' | 'pcbE' | 'pcbF' | 'pcbG' | 'pcbH')"
                gene_names = set(re.findall(r"'(\w+)'", unwrapped_rule))
                # remove the rule name from the set and add the unwrapped rule expression
                trait_rule_unwrapped_mapping[trait_name].remove(rule_name)
                trait_rule_unwrapped_mapping[trait_name].update(gene_names)
            else:
                raise ValueError(f"Rule name {rule_name} not found in ruleunwrapped file")

    if trait_unwrapped_rules_file is not None:
        with open(trait_unwrapped_rules_file, 'w') as file:
            file.write(f"{SYS_TRAIT_ID}\t{_UNWRAPPED_GENE_COL}\n")
            for trait, rule_set in trait_rule_unwrapped_mapping.items():
                # Join the set elements into a single string
                unwrapped_rules = ';'.join(rule_set)
                file.write(f"{trait}\t{unwrapped_rules}\n")

    return trait_rule_unwrapped_mapping
