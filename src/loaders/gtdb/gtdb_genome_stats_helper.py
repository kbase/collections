"""
Methods in this script are designed to compute a genome feature.

Methods accept a pandas dataframe and should return a pandas series containing only the computed genome feature. It will
then be appended to the result frame.
"""

# genome statistics already existing in the metadata files
EXIST_FEATURES = {'accession', 'checkm_completeness', 'checkm_contamination', 'checkm_marker_count',
                  'checkm_marker_lineage', 'checkm_marker_set_count', 'contig_count', 'gc_count', 'gc_percentage',
                  'genome_size', 'gtdb_taxonomy', 'longest_contig', 'longest_scaffold', 'mean_contig_length',
                  'mean_scaffold_length', 'mimag_high_quality', 'mimag_low_quality', 'mimag_medium_quality',
                  'n50_contigs', 'n50_scaffolds', 'ncbi_assembly_level', 'ncbi_assembly_name', 'ncbi_bioproject',
                  'ncbi_biosample', 'ncbi_country', 'ncbi_date', 'ncbi_genbank_assembly_accession',
                  'ncbi_genome_category', 'ncbi_isolate', 'ncbi_isolation_source', 'ncbi_lat_lon', 'ncbi_organism_name',
                  'ncbi_seq_rel_date', 'ncbi_species_taxid', 'ncbi_strain_identifiers', 'ncbi_submitter', 'ncbi_taxid',
                  'ncbi_taxonomy_unfiltered', 'protein_count', 'scaffold_count', 'ssu_count', 'ssu_length',
                  'trna_aa_count', 'trna_count', 'trna_selenocysteine_count'}


# TODO: prototype method should remove
def high_checkm_marker_count(df):
    sr = df['checkm_marker_count'] > 150
    sr.rename('high_checkm_marker_count', inplace=True)
    return sr
