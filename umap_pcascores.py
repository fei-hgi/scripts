import hail as hl
import pyspark

import umap
import numpy as np
import matplotlib.pyplot as plt

tmp_dir = "hdfs://spark-master:9820/"
sc = pyspark.SparkContext()
hadoop_config = sc._jsc.hadoopConfiguration()
hl.init(sc=sc, tmp_dir=tmp_dir, default_reference="GRCh38")

mtdir = "file:///lustre/scratch123/teams/hgi/fs18/ddd_variant_qc/sample_qc/matrixtables/"
pca_score_ht_file = mtdir + "mt_pca_scores.ht"
pca_score_ht = hl.read_table(pca_score_ht_file)

figdir = "/lustre/scratch123/teams/hgi/fs18/ddd_variant_qc/sample_qc/figures/"

#pca_score_ht.show(n=5)

pca_score_df = pca_score_ht.to_pandas()
pca_score_nplist = pca_score_df.scores.to_numpy()
pca_score_nparray = np.vstack(pca_score_nplist).astype(np.float)

pca_score_umap = umap.UMAP(n_components = 2, n_neighbors = 15, min_dist = 0.1, metric = 'euclidean').fit_transform(pca_score_nparray[:,:10])

plt.scatter(pca_score_umap[:,0], pca_score_umap[:,1], s = 2, c = 'blue', marker = 'o', alpha = 0.5, linewidths = 0.1, edgecolors = None)
plt.savefig(figdir + "umap_pcascores.png")
plt.clf()

pop_ht_file = mtdir + "pop_assignments.ht"
pop_ht = hl.read_table(pop_ht_file)


