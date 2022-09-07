import hail as hl
import pyspark
import argparse
import os
import shutil
import stat

tmp_dir = "file:///lustre/scratch123/teams/hgi/fs18/ddd_qc/results_afterqc/tmp/"
sc = pyspark.SparkContext()
hadoop_config = sc._jsc.hadoopConfiguration()
hl.init(sc=sc, tmp_dir=tmp_dir, default_reference="GRCh38")

mt_pavlos = hl.read_matrix_table("file:///lustre/scratch123/mdt2/teams/hgi/fs18/ddd_qc/results_pavlos/matrixtables/gatk_unprocessed_with_pop_and_runid_with_cq.mt")
mt_ddd = hl.read_matrix_table("file:///lustre/scratch123/teams/hgi/fs18/ddd_qc/variant_qc/matrixtables/mt_varqc_splitmulti_with_cq_and_rf_scores_299d023c.mt")

samples_to_keep = mt_ddd.s.collect()
set_to_keep = hl.literal(samples_to_keep)
mt_pavlos_filter = mt_pavlos.filter_cols(set_to_keep.contains(mt_pavlos.s), keep=True)

mt_tmp = mt_pavlos_filter
sample_list = mt_tmp.cols().s.collect()
samples = dict.fromkeys(sample_list)
for s in samples:
    samples[s] = {}

mt_snp = mt_tmp.filter_rows(hl.is_snp(mt_tmp.alleles[0], mt_tmp.alleles[1]))
mt_cq_all = mt_snp.filter_rows(mt_snp.info.consequence == 'missense_variant')

mt_cq = mt_cq_all.filter_rows(mt_cq_all.info.bin <= 90)
mt_cq = hl.sample_qc(mt_cq)
sampleqc_ht = mt_cq.cols()

non_ref = sampleqc_ht.sample_qc.n_non_ref.collect()
sampleqc_samples = sampleqc_ht.s.collect()
sample_counts = {sampleqc_samples[i]: non_ref[i] for i in range(len(sampleqc_samples))}


