import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

os.chdir("/lustre/scratch123/teams/hgi/mercury/Genes_and_Health_2022_08_27k/bcftools_gtcheck/gh_isec_vcfs/")

df = pd.read_csv("gtcheck_top_5_matches.input.txt", sep = '\t')

df_plot = {}
for index, row in df.iterrows():
    if index%5 == 0:
        df_plot[row["Query"]] = [row["Sites"]]
        df_plot[row["Query"]].append(row["Discordance"])
    else:
        df_plot[row["Query"]].append(row["Discordance"])

plotcol = ['grey', 'red', 'yellowgreen', 'royalblue', 'violet']
plotlab = ['1st', '2nd', '3rd', '4th', '5th']

df_plot_all = []
for i in range(1,6):
    df_plot_n = [x[i] for x in df_plot.values()]
    df_plot_n_log = np.log10(df_plot_n)
    df_plot_all.append(df_plot_n_log)

plt.title("Discordance")
plt.ylabel("log10(Discordance)")
bp = plt.boxplot(df_plot_all, patch_artist = True, 
                 flierprops={'alpha':0.25, 
                             'markersize': 20,
                             'markeredgecolor': 'None',
                             'marker': '.'})
for patch, color in zip(bp['boxes'], plotcol):
    patch.set_facecolor(color)
for f, color in zip(bp['fliers'], plotcol):
    f.set_markerfacecolor(color)
plt.xticks([1,2,3,4,5], plotlab)
plt.savefig("gtcheck_top_5_matches.discordance_boxplot.png")

plt.title("Sorted Discordance")
plt.xlabel("Sorted Index")
plt.ylabel("log10(Discordance)")
for i in range(1,6):
    df_plot_n = [x[i] for x in df_plot.values()]
    df_plot_n_log = np.log10(df_plot_n)
    df_plot_n_log.sort()
    plt.scatter(range(df_plot_n_log.size), df_plot_n_log, color = plotcol[i-1], label = plotlab[i-1], s = 10, alpha = 0.5)
plt.legend(loc = "center right", bbox_to_anchor = (0,1,1,-0.7))
plt.savefig("gtcheck_top_5_matches.discordance.png")

plt.title("Discordance and Sites")
plt.xlabel("No. of Sites")
plt.ylabel("log10(Discordance)")
df_plot_sites = [x[0] for x in df_plot.values()]
for i in range(1,6):
    df_plot_n = [x[i] for x in df_plot.values()]
    df_plot_n_log = np.log10(df_plot_n)
    plt.scatter(df_plot_sites, df_plot_n_log, color = plotcol[i-1], label = plotlab[i-1], s = 10, alpha = 0.5)
plt.legend(loc = "center right", bbox_to_anchor = (0,1,1,-0.7))
plt.savefig("gtcheck_top_5_matches.discordance_sites.png")

df_plot_n2 = [x[2] for x in df_plot.values()]
df_plot_n2_log = np.log10(df_plot_n2)
df_plot_n2_log_b0 = []
for x in df_plot_n2_log:
  if x < 0:
    df_plot_n2_log_b0.append(True)
  else:
    df_plot_n2_log_b0.append(False)
df_plot_n2_log_b0f = df_plot_n2_log[df_plot_n2_log_b0]

df_plot_s = [x[0] for x in df_plot.values()]
df_plot_s = np.array(df_plot_s)
df_plot_s_log_b0f = df_plot_s[df_plot_n2_log_b0]

df_plot_key = [x for x in df_plot.keys()]
df_plot_key = np.array(df_plot_key)
df_plot_key_log_b0f = df_plot_key[df_plot_n2_log_b0]

df_plot_input = []
for i in range(1,6):
    df_plot_n = [x[i] for x in df_plot.values()]
    df_plot_n_log = np.log10(df_plot_n)
    df_plot_input.append(df_plot_n_log[df_plot_n2_log_b0])

fig, (ax1, ax2) = plt.subplots(1, 2)
fig.suptitle('Discordance (filtered by 2nd discordance score)')
bp = ax1.boxplot(df_plot_input, patch_artist = True, 
                 flierprops={'alpha':0.5, 
                             'markersize': 10,
                             'markeredgecolor': 'None',
                             'marker': '.'})
for patch, color in zip(bp['boxes'], plotcol):
    patch.set_facecolor(color)
for f, color in zip(bp['fliers'], plotcol):
    f.set_markerfacecolor(color)
ax1.set(ylabel = "log10(Discordance)", xticklabels = plotlab)
bp = ax2.boxplot(df_plot_input, patch_artist = True, 
                 flierprops={'alpha':0.5, 
                             'markersize': 10,
                             'markeredgecolor': 'None',
                             'marker': '.'})
for patch, color in zip(bp['boxes'], plotcol):
    patch.set_facecolor(color)
for f, color in zip(bp['fliers'], plotcol):
    f.set_markerfacecolor(color)
ax2.set(ylabel = "log10(Discordance)", xticklabels = plotlab)
ax2.set_ylim([-0.5,0.5])
plt.savefig("gtcheck_top_5_matches.discordance_boxplot_filtered.png")

df_log_b0f = df.query("Query in @df_plot_key_log_b0f")
df_log_b0f.to_csv(r'gtcheck_top_5_matches.filtered.txt', index = None, sep = '\t', mode = 'a')
