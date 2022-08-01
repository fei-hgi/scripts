import hail as hl
import pyspark

tmp_dir = "hdfs://spark-master:9820/"
sc = pyspark.SparkContext()
hadoop_config = sc._jsc.hadoopConfiguration()
hl.init(sc=sc, tmp_dir=tmp_dir, default_reference="GRCh38")

mt = hl.read_matrix_table("file:///lustre/scratch123/teams/hgi/fs18/ddd_variant_qc/variant_qc/matrixtables/mt_varqc_splitmulti.mt")
ht = hl.import_table("file:///lustre/scratch123/teams/hgi/fs18/ddd_variant_qc/resources/vepanno_variants.txt", no_header=True, impute = True)

ht = ht.annotate(chr=ht.f0)
ht = ht.annotate(pos=ht.f1)
ht = ht.annotate(rs=ht.f2)
ht = ht.annotate(ref=ht.f3)
ht = ht.annotate(alt=ht.f4)
ht = ht.annotate(consequence=ht.f5)
ht = ht.annotate(impact=ht.f6)
ht = ht.annotate(lof=ht.f7)

ht = ht.key_by(locus=hl.locus(ht.chr, ht.pos), alleles=[ht.ref,ht.alt])
ht = ht.drop(ht.f0, ht.f1, ht.f2, ht.f3, ht.f4, ht.f5, ht.f6, ht.f7, ht.chr, ht.pos, ht.ref, ht.alt)
ht = ht.key_by(ht.locus, ht.alleles)

mt = mt.annotate_rows(consequence = ht[mt.row_key].consequence)
mt = mt.annotate_rows(lof = ht[mt.row_key].lof)

