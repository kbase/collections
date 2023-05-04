import pytest

from src.loaders.compute_tools.microtrait.microtrait_util import decode_rule_boolean, retrieve_detected_genes


def test_decode_rule_boolean():
    """
    Test cases parsed from rules_asserted table.
       microtrait_rule-name            microtrait_rule-boolean  \
    7            PGAP<->3PG                            ('pgk')
    8            G3P<->PGAP       ('gapA.NAD+' | 'gapA.NADP+')
    14         cc_reduction  (('PGAP<->3PG') & ('G3P<->PGAP'))

       microtrait_rule-booleanunwrapped_set microtrait_rule-asserted
    7                                   (1)                     True
    8                               (1 | 0)                     True
    14                  (((1)) & ((1 | 0)))                     True
    """
    rule_name_boolean_dict = {'PGAP<->3PG': "('pgk')",
                              'G3P<->PGAP': "('gapA.NAD+' | 'gapA.NADP+')",
                              'cc_reduction': "(('PGAP<->3PG') & ('G3P<->PGAP'))"}

    # decoded boolean rules' parentheses should match exactly the microtrait_rule-booleanunwrapped_set column
    decoded_cc_reduction = decode_rule_boolean('cc_reduction', rule_name_boolean_dict)
    assert decoded_cc_reduction == "((('pgk')) & (('gapA.NAD+' | 'gapA.NADP+')))"

    # rules already at the gene(lowest) level of boolean expression should be returned as is
    for rule_name in ['PGAP<->3PG', 'G3P<->PGAP']:
        assert rule_name_boolean_dict.get(rule_name) == decode_rule_boolean(rule_name, rule_name_boolean_dict)

    """
    Test cases parsed from rules_asserted table.
       microtrait_rule-name microtrait_rule-boolean  \
    6         RuBP+CO2->3PG       ('rbcS' | 'rbcL')   
    18       cc_CO2fixation       ('RuBP+CO2->3PG')   
    
       microtrait_rule-booleanunwrapped_set microtrait_rule-asserted  
    6                               (0 | 1)                     True  
    18                            ((0 | 1))                     True  
    """
    rule_name_boolean_dict = {'RuBP+CO2->3PG': "('rbcS' | 'rbcL')",
                              'cc_CO2fixation': "('RuBP+CO2->3PG')"}

    decoded_cc_CO2fixation = decode_rule_boolean('cc_CO2fixation', rule_name_boolean_dict)
    assert decoded_cc_CO2fixation == "(('rbcS' | 'rbcL'))"
    assert rule_name_boolean_dict.get('RuBP+CO2->3PG') == decode_rule_boolean('RuBP+CO2->3PG', rule_name_boolean_dict)

    """
    Test cases parsed from rules_asserted table.
         microtrait_rule-name microtrait_rule-boolean  \
    211        flagella_motor   (('motA') | ('motB'))   
    1142                 motA                ('motA')   
    1143                 motB                ('motB')   
    
         microtrait_rule-booleanunwrapped_set microtrait_rule-asserted  
    211   ((((((((1))))))) | (((((((1))))))))                     True  
    1142                      (((((((1)))))))                     True  
    1143                      (((((((1)))))))                     True  
    """
    rule_name_boolean_dict = {'flagella_motor': "(('motA') | ('motB'))",
                              'motA': "('motA')",
                              'motB': "('motB')"}

    # rule boolean itself is another rule
    for rule_name in ['motA', 'motB']:
        assert f"((((((('{rule_name}')))))))" == decode_rule_boolean(rule_name, rule_name_boolean_dict)

    rule_name = 'flagella_motor'
    decoded_flagella_motor = decode_rule_boolean(rule_name, rule_name_boolean_dict)
    assert decoded_flagella_motor == "(((((((('motA'))))))) | ((((((('motB'))))))))"

    """
    Test cases parsed from rules_asserted table.
         microtrait_rule-name microtrait_rule-boolean  \
    355             nirBC_and       ('nirB' & 'nirC')   
    1176                 nirC                ('nirC')   
    
         microtrait_rule-booleanunwrapped_set microtrait_rule-asserted  
    355                   (1 & ((((((0)))))))                    False  
    1176                      (((((((0)))))))                    False 
    """
    rule_name_boolean_dict = {'nirBC_and': "('nirB' & 'nirC')",
                              'nirC': "('nirC')"}

    rule_name = 'nirBC_and'
    decoded_nirBC_and = decode_rule_boolean(rule_name, rule_name_boolean_dict)
    assert decoded_nirBC_and == "('nirB' & (((((('nirC')))))))"

    """
    Test cases parsed from rules_asserted table.
          microtrait_rule-name    microtrait_rule-boolean  \
    176  mannose6P->fructose6P                   ('manA')   
    177    mannose degradation  ('mannose6P->fructose6P')   
    
        microtrait_rule-booleanunwrapped_set microtrait_rule-asserted  
    176                                  (0)                    False  
    177                                ((0))                    False  
    """
    rule_name_boolean_dict = {'mannose6P->fructose6P': "('manA')",
                              'mannose degradation': "('mannose6P->fructose6P')"}

    rule_name = 'mannose degradation'
    decoded_mannose = decode_rule_boolean(rule_name, rule_name_boolean_dict)
    assert decoded_mannose == "(('manA'))"


def test_retrieve_detected_genes():
    decoded_rule_boolean = "('nirB' & (((((('nirC')))))))"
    booleanunwrapped_set = "(1 & ((((((0)))))))"
    detected_genes = retrieve_detected_genes(decoded_rule_boolean, booleanunwrapped_set)
    assert detected_genes == ['nirB']

    decoded_rule_boolean = "(((((((('motA'))))))) | ((((((('motB'))))))))"
    booleanunwrapped_set = "((((((((1))))))) | (((((((1))))))))"
    detected_genes = retrieve_detected_genes(decoded_rule_boolean, booleanunwrapped_set)
    assert detected_genes == ['motA', 'motB']

    decoded_rule_boolean = "('pgk')"
    booleanunwrapped_set = "(1)"
    detected_genes = retrieve_detected_genes(decoded_rule_boolean, booleanunwrapped_set)
    assert detected_genes == ['pgk']

    with pytest.raises(ValueError):
        retrieve_detected_genes("(('pgk')", "(1)")

    with pytest.raises(ValueError):
        retrieve_detected_genes("('pgk'))", "(0)")

    with pytest.raises(ValueError):
        retrieve_detected_genes("('pgk')", "((0))")
