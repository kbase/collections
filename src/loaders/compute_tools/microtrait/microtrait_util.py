def decode_rule_boolean(
        rule_name: str,
        rule_name_boolean_dict: dict[str, str],
        depth: int = 5,
) -> str:
    """
    Given a rule name and a dictionary of rule names and their corresponding boolean expressions,
    return the boolean expression at the gene level for the given rule name.
    """

    if rule_name not in rule_name_boolean_dict:
        return ''

    rule_boolean = rule_name_boolean_dict[rule_name]

    # Replace any existing rule names with their corresponding boolean expressions
    for k, v in rule_name_boolean_dict.items():

        # special case when rule boolean is another rule
        if f"('{rule_name}')" in rule_boolean:
            rule_boolean = rule_boolean.replace(f"('{rule_name}')", f"((((((('{rule_name}')))))))")
            return rule_boolean

        if k in rule_boolean:
            rule_boolean = rule_boolean.replace(f"'{k}'", f"{v}")

    # Recursively call the function until there are no more substitutions to make
    if any(k in rule_boolean for k in rule_name_boolean_dict) and depth > 0:
        rule_name_boolean_dict[rule_name] = rule_boolean
        rule_boolean = decode_rule_boolean(rule_name, rule_name_boolean_dict, depth=depth - 1)

    return rule_boolean


def retrieve_detected_genes(
        decoded_rule_boolean: str,
        booleanunwrapped_set: str
) -> list[str]:
    """
    retrieve detected genes from decoded_rule_boolean where the corresponding position in the
    booleanunwrapped_set is 1(True)
    """
    detected_genes, j = list(), 0
    unmatch_msg = f"decoded rule boolean [{decoded_rule_boolean}] does not match booleanunwrapped_set string [{booleanunwrapped_set}]"
    for i, c in enumerate(booleanunwrapped_set):

        # retrieve corresponding gene name in the decoded rule boolean
        if c in ['1', '0']:
            first_quote_position = j
            start_position = first_quote_position + 1
            end_position = decoded_rule_boolean.find("'", start_position)
            substring = decoded_rule_boolean[start_position:end_position]

            if c == '1':
                detected_genes.append(substring)
            j += len(substring) + 1
        else:
            # check if the character matches the decoded rule boolean
            if c != decoded_rule_boolean[j]:
                raise ValueError(unmatch_msg)

        j += 1

    # check if the decoded rule boolean is fully traversed
    if j != len(decoded_rule_boolean):
        raise ValueError(unmatch_msg)

    return detected_genes
